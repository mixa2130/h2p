"""https://dev.to/yugabyte/postgres-query-execution-jdbc-prepared-statements-51e2"""
import logging
import sys
import typing as tp
from urllib.parse import quote

import asyncpg
from pyspark.sql import SparkSession
from tenacity import retry, wait_fixed, stop_after_attempt

from logger import ML360Logger


class PgCreds(tp.NamedTuple):
    username: str
    password: str
    jdbc_uri: str
    pg_dsn: str


RECONNECT_ATTEMPTS = 3
WAIT_SECONDS = 10


class AppContext:
    def __init__(self):
        self.secrets: dict = {x.split("=")[0].lower().strip(): x.split("=")[1].strip() for x in sys.argv[1:]}

        self.debug_mode = self.get_secret('debug')
        self.logger: ML360Logger = ML360Logger(level=logging.INFO if not self.debug_mode else logging.DEBUG)

        self._jsc_conf = None
        self._spark: tp.Optional[SparkSession] = None
        self._pg_pool: tp.Optional[asyncpg.Pool] = None

    @property
    def spark(self):
        return self._spark

    @property
    def pg_pool(self):
        return self._pg_pool

    @property
    def debug_mode(self) -> bool:
        return self._debug_mode

    @debug_mode.setter
    def debug_mode(self, val: tp.Optional[str]):
        self._debug_mode: bool = False
        if val is not None and val.lower() == 'true':
            self._debug_mode = True

    def _set_spark_session(self):
        self._spark = SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()

    @staticmethod
    def get_pg_dsn(jdbc_uri: str, username: str, password: str):
        _host = jdbc_uri.split('://')[-1]
        return ''.join(['postgresql://', ':'.join([quote(username), quote(password)]), '@', _host])

    def get_jceks_cred(self, jsc_alias) -> str:
        """
        :param jsc_alias: credential from jceks, which was taken by alias
        """
        jsc_cred = self._jsc_conf.getPassword(jsc_alias)
        return ''.join((str(jsc_cred.__getitem__(i)) for i in range(jsc_cred.__len__())))

    def get_secret(self, key: str) -> tp.Optional[str]:
        secret: tp.Optional[str] = self.secrets.get(key)

        if secret is None or secret.lower() == 'empty':
            return None
        return secret.strip()

    @retry(stop=stop_after_attempt(RECONNECT_ATTEMPTS), wait=wait_fixed(WAIT_SECONDS))
    async def start_asyncpg_pool(self, pg_dsn: str, min_size: int = 1, max_size: int = 2):
        self._pg_pool = await asyncpg.create_pool(pg_dsn, min_size=min_size, max_size=max_size, statement_cache_size=0)
        self.logger.debug('Started pg pool')

    async def on_startup(self) -> PgCreds:
        # Spark
        self._set_spark_session()
        self.logger.debug("Started spark session")

        # Logger
        try:
            self.logger.activate_spark_logger(spark_context=self._spark,
                                              logger_name='ExportToPostgres')
        except Exception as exc:
            self.logger.warning("Unable to activate spark logger {}".format(repr(exc)))

        # Jceks
        self._jsc_conf = self._spark._jsc.hadoopConfiguration()
        self._jsc_conf.set('hadoop.security.credential.provider.path', self.get_secret('jceks_provider'))

        pg_password: str = self.get_jceks_cred(self.get_secret('jceks_password_alias'))
        jdbc_uri: str = self.get_secret('postgres_jdbc_url')
        pg_user: str = self.get_secret('tuz_name')
        pg_dsn = self.get_pg_dsn(jdbc_uri=jdbc_uri,
                                 username=pg_user,
                                 password=pg_password)

        # Async Pg connection pool
        await self.start_asyncpg_pool(pg_dsn)

        return PgCreds(jdbc_uri=jdbc_uri + "?prepareThreshold=0",
                       username=pg_user,
                       password=pg_password,
                       pg_dsn=pg_dsn)

    async def close_asyncpg_pool(self):
        if self._pg_pool is not None:
            await self._pg_pool.close()
            self.logger.debug("Successfully closed pg pool")

    def close_spark_session(self):
        if self._spark is not None:
            self._spark.stop()
            self.logger.debug("Successfully closed spark session")

    async def on_shutdown(self):
        self.logger.debug("Destructor")

        await self.close_asyncpg_pool()
        self.logger.deactivate_spark_logger()
        self.close_spark_session()
