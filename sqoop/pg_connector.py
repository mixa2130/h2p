import typing as tp

from asyncpg.pool import Pool as AsyncPgPool
from asyncpg.connection import Connection as AsyncPgConnection
from tenacity import retry, wait_fixed, stop_after_attempt

from context import AppContext
from logger import ML360Logger

RECONNECT_ATTEMPTS = 3
WAIT_SECONDS = 5


@retry(reraise=True, stop=stop_after_attempt(RECONNECT_ATTEMPTS), wait=wait_fixed(WAIT_SECONDS))
async def check_table_existence(session: [AsyncPgPool, AsyncPgConnection], schema: str, table: str) -> bool:
    """
    Checks the routine existence in specified schema.

    :param session: asyncpg pool/connection
    :param schema: schema in postgres
    :param table: routine name in postgres
    """
    check_sql = f"""SELECT EXISTS(SELECT
                                 FROM pg_catalog.pg_tables
                                 WHERE schemaname = '{schema}'
                                 AND tablename = '{table}')"""

    routine_check = await session.fetch(check_sql)
    return routine_check[0][0]


@retry(reraise=True, stop=stop_after_attempt(RECONNECT_ATTEMPTS), wait=wait_fixed(WAIT_SECONDS))
async def update_table_privileges(session: tp.Union[AsyncPgPool, AsyncPgConnection], logger: ML360Logger,
                                  schema_with_tbl: str):
    """
    Gives table owner privileges to 'as_admin' role.
    Grants select privilege to as_TUZ role.

    :param session: asyncpg pool/connection
    :param logger:
    :param schema_with_tbl: postgre table(schema.name)
    """
    await session.execute(f'ALTER TABLE {schema_with_tbl} OWNER TO "as_admin"')
    await session.execute(f'GRANT SELECT ON {schema_with_tbl} TO "as_TUZ"')

    logger.info(f"Privileges on {schema_with_tbl} has been successfully granted")


@retry(reraise=True, stop=stop_after_attempt(RECONNECT_ATTEMPTS), wait=wait_fixed(WAIT_SECONDS))
async def create_backup(context: AppContext, schema: str, table: str, backup_prefix: str = 'backup_'):
    context.logger.debug(f"Start ASYNC create backup for {schema}.{table}")

    backup_nm = backup_prefix + table
    backup_tbl = f"{schema}.{backup_nm}"
    main_tbl = f"{schema}.{table}"

    async with context.pg_pool.acquire() as session:
        async with session.transaction():
            check_tbl_exist: bool = await check_table_existence(session, schema, backup_nm)

            if not check_tbl_exist:
                await session.execute(f"CREATE TABLE {backup_tbl} AS SELECT * FROM {main_tbl}")
                context.logger.debug("Backup created")
                await update_table_privileges(session=session, logger=context.logger, schema_with_tbl=backup_tbl)
            else:
                await session.execute(f"TRUNCATE {backup_tbl}")
                await session.execute(f"INSERT INTO {backup_tbl} SELECT * FROM {main_tbl}")
                context.logger.debug("Backup updated with actual data")

    context.logger.info(f"Successfully saved backup at {backup_tbl}")


@retry(reraise=True, stop=stop_after_attempt(RECONNECT_ATTEMPTS), wait=wait_fixed(WAIT_SECONDS))
async def change_partitions(context: AppContext, schema: str, master_table: str, slave_table: str):
    """
    :param context: app context
    :param schema: postgre schema name
    :param master_table: table name from where to write
    :param slave_table: table name where to write
    """
    context.logger.debug(f"Start ASYNC change_partitions {master_table}->{slave_table}")

    async with context.pg_pool.acquire() as session:
        async with session.transaction():
            _master = f"{schema}.{master_table}"
            _slave = f"{schema}.{slave_table}"

            await session.execute(f"TRUNCATE {_slave}")
            await session.execute(f"INSERT INTO {_slave} SELECT * FROM {_master}")

    context.logger.info(f"Successfully updated {slave_table} table with actual data")


@retry(reraise=True, stop=stop_after_attempt(RECONNECT_ATTEMPTS), wait=wait_fixed(WAIT_SECONDS))
async def drop_tbl_if_exists(context: AppContext, schema_with_tbl: str):
    context.logger.debug(f"Start ASYNC drop table {schema_with_tbl}")

    async with context.pg_pool.acquire() as session:
        async with session.transaction():
            await session.execute(f"DROP TABLE IF EXISTS {schema_with_tbl}")

    context.logger.info(f"Successfully dropped {schema_with_tbl}")
