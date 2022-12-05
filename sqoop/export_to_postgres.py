"""
RELEASE NOTES:
    2.0:
        § refused from schema_tmp parameter
        § refused from change_partitions function
        § inverted value for do_crash_on_postgres_connection_error
        § expanded debug - also changes logging level

Griffon, by default, adds 'EMPTY' to not specified values

CTL params:

REQUIRED:
    - tuz_name: postgres user - tuz.
        - Example: "ci_ci02145093"
    - jceks_provider: jceks storage path in hdfs.
        - Example: "jceks://hdfs/user/stupak-mv_ca-sbrf-ru/local.jceks"
    - jceks_password_alias: jceks alias for postgres password.
        - Example: "postgres.password"
    - postgres_jdbc_url: postgres jdbc connection url
        Example: "jdbc:postgresql://tklid-pcap00016.vm.mos.cloud.sbrf.ru:5433/ml360"

    - hive_schema: the schema in HIVE, from where we export data to PostgreSQL.
        - Example: "custom_cib_ml360"
    - hive_table: the table in HIVE, from where we export data to PostgreSQL.
        - Example: "test_table"
    - postgres_schema: the schema in PostgreSQL
        - Example: "custom_cib_ml360"
    - postgres_table: the table name in PostgreSQL.
        - Example: "test_table"

OPTIONAL:
    - postgres_types: specify types for mapping during export hive->postgres
        - Example: "key: integer; username: text; created_on: timestamp with time zone"
        default: "EMPTY"
    - max_partition:
        - true: export max partition
        default: false

    - debug:
        - true: don't truncate tmp table after export
        default: false
    - do_crash_on_postgres_connection_error: there are no connection to postgres from uat - this flag allows escaping
     PostgresConnectionError and finish execution successfully
        - Example: "false"
        default: "true"
    - sample_data: allows to sent synthetic through ctl params. Such dataframe will be recognized with dtypes=str
        - Example: '{"columns":["col 1","col 2"],"index":["row 1","row 2"],"data":[["a","b"],["c","d"]]}'
        default: "EMPTY"

It was decided to put the log4j file next to the export script by user.
You can use template, but don't forget to change appid!
"""
import json
import typing as tp

import asyncio
import pandas as pd
import tenacity
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

import pg_connector as pgFuncs
from context import AppContext, PgCreds

# Repartition will allow us to open 5 connections.
# 5 is optimal for 2 reasons:
# 1) minimum number of postgres connections is 100
# 2) minimum number of postgres connections per user is 5
PG_JDBC_LIMIT = 5


class ExportToPostgres:
    def __init__(self, context: AppContext, pg_creds: PgCreds):
        self.context: AppContext = context
        self.pg_creds: PgCreds = pg_creds

    @staticmethod
    def mapping_types(spark_df: SparkDataFrame, user_types_mapping: tuple, logger=None) -> \
            tp.Tuple[SparkDataFrame, str]:
        """
        Produces types mapping from Hive to PostgreSql

        :param spark_df: spark dataframe
        :param user_types_mapping: specify types for mapping during export hive->postgres
        :param logger: logger if it's specified

        :return: (spark dataframe with mapped types,
            string from attributes and their types for PostgreSql)
        """
        user_pg_types = dict()
        spark_df_types = spark_df.dtypes

        for el in user_types_mapping:
            el_key, el_val = el.split(':')
            user_pg_types[el_key.strip().lower()] = el_val.strip().lower()

        _column_types = []
        for cl_name, cl_type in map(lambda x: (x[0].lower(), x[1].lower()), spark_df_types):
            _attr_type: tp.Optional[str] = None

            in_user_types: tp.Optional[str] = user_pg_types.get(cl_name)
            if in_user_types is not None:
                # text will be processed at the final step with other types
                # like 'tinyint' and 'string'

                if in_user_types != 'text':
                    _attr_type = in_user_types

            elif cl_type in ('smallint', 'int', 'integer'):
                _attr_type = 'integer'
            elif cl_type == 'bigint':
                _attr_type = 'bigint'
            elif cl_type in ('float', 'numeric', 'double', 'double precision') or cl_type[:7] == 'decimal':
                _attr_type = 'float'

            if _attr_type is not None:
                _column_types.append(' '.join((cl_name, _attr_type)))
            else:
                # 'tinyint', 'binary', 'array', 'string', 'timestamp' and etc.
                # will be automatically converted to 'text' in Postgres

                spark_df = spark_df.withColumn(cl_name, col(cl_name).cast(StringType()))
                if logger is not None:
                    logger.debug(f"Cast {cl_name} to text")

        return spark_df, ', '.join(_column_types)

    def get_max_partition(self, hive_table: str):
        """
        Allows getting max partition condition from hive_table.
        Should be used with tables partitioned by one attribute.

        :param hive_table: table with schema in Hive

        :return: value of last partition
        :rtype: str
        """

        def format_partition_val(val):
            els = val.split('=')
            return "{0}='{1}'".format(*els)

        raw_max_partition = self.context.spark.sql(f"show partitions {hive_table}").rdd.flatMap(lambda x: x).max()
        return ' AND '.join(map(format_partition_val, raw_max_partition.split('/')))

    def spark_to_pg_tbl(self, spark_df: SparkDataFrame, pg_schema_with_tbl: str):
        user_types_mapping = tuple()
        if self.context.get_secret('postgres_types') is not None:
            user_types_mapping = self.context.get_secret('postgres_types').split(';')

        sparkDf, createTableColumnTypes = self.mapping_types(user_types_mapping=user_types_mapping,
                                                             spark_df=spark_df)

        self.context.logger.info(f"Types after mapping: {createTableColumnTypes}")
        sparkDf.write.mode("overwrite") \
            .option("createTableColumnTypes", createTableColumnTypes) \
            .format("jdbc") \
            .option("url", self.pg_creds.jdbc_uri) \
            .option("truncate", True) \
            .option('dbtable', pg_schema_with_tbl) \
            .option('batchsize', 10000) \
            .option("numPartitions", PG_JDBC_LIMIT) \
            .option("user", self.pg_creds.username) \
            .option("password", self.pg_creds.password) \
            .save()
        self.context.logger.info(f"Successfully saved {pg_schema_with_tbl} in postgres")

    async def async_spark_to_pg_tbl(self, spark_df: SparkDataFrame, pg_schema_with_tbl: str):
        soup_loop = asyncio.get_running_loop()
        return await soup_loop.run_in_executor(None, self.spark_to_pg_tbl, spark_df, pg_schema_with_tbl)

    async def run(self, hive_table_with_schema: str, pg_schema: str, pg_table: str, max_partition: bool = False):
        custom_df = self.context.get_secret('sample_data')

        self.context.logger.info(
            f"Start export from {hive_table_with_schema} in HIVE to {pg_schema}.{pg_table} in PostgreSQl")

        # Get data from Hive to export them to Postgres
        if custom_df is None:
            sparkDf = self.context.spark.table(hive_table_with_schema)

            if max_partition:
                partition_val = self.get_max_partition(hive_table_with_schema)
                self.context.logger.info("Max partition mode: {}".format(partition_val))

                sparkDf = sparkDf.where(partition_val)
            sparkDf = sparkDf.repartition(PG_JDBC_LIMIT)
        else:
            self.context.logger.info("Sample dataframe accepted")

            _pd_df: pd.DataFrame = pd.DataFrame.from_dict(json.loads(custom_df), dtype='str')
            sparkDf: SparkDataFrame = self.context.spark.createDataFrame(_pd_df.astype('str'))

        tbl_exists: bool = await pgFuncs.check_table_existence(session=self.context.pg_pool,
                                                               schema=pg_schema,
                                                               table=pg_table)
        if not tbl_exists:
            self.context.logger.info(f'Table {pg_schema}.{pg_table} does not exist')

            # Spark to main table
            main_tbl = f'{pg_schema}.{pg_table}'
            await self.async_spark_to_pg_tbl(spark_df=sparkDf, pg_schema_with_tbl=main_tbl)
            await pgFuncs.update_table_privileges(session=self.context.pg_pool, logger=self.context.logger,
                                                  schema_with_tbl=main_tbl)
        else:
            self.context.logger.info(f'Table {pg_schema}.{pg_table} already exists - data will be updated')
            tmp_table = f"tmp_{pg_table}"
            tmp_table_with_schema = f"{pg_schema}.{tmp_table}"

            # Save backup and tmp_table
            await asyncio.gather(pgFuncs.create_backup(context=self.context, schema=pg_schema, table=pg_table),
                                 self.async_spark_to_pg_tbl(spark_df=sparkDf, pg_schema_with_tbl=tmp_table_with_schema))

            # Update data in main table
            await asyncio.gather(
                pgFuncs.update_table_privileges(session=self.context.pg_pool, logger=self.context.logger,
                                                schema_with_tbl=tmp_table_with_schema),
                pgFuncs.change_partitions(context=self.context, schema=pg_schema,
                                          master_table=tmp_table, slave_table=pg_table)
                )

            if not self.context.debug_mode:
                await pgFuncs.drop_tbl_if_exists(context=self.context, schema_with_tbl=tmp_table_with_schema)


async def main():
    from datetime import datetime

    context = AppContext()
    start_time = datetime.now()

    try:
        pg_creds: PgCreds = await context.on_startup()
        exporter = ExportToPostgres(context, pg_creds)
    except tenacity.RetryError as exc:
        context.logger.warning("Postgres is unavailable")
        fail_in_err: tp.Optional[str] = context.get_secret("do_crash_on_postgres_connection_error")
        await context.on_shutdown()

        if fail_in_err is None or fail_in_err.lower() == 'true':
            raise exc

        context.logger.warning("Unable to establish Postgre connection, "
                               "do_crash_on_postgres_connection_error is disabled")
        return None

    try:
        hive_schema = context.get_secret('hive_schema')
        hive_table = context.get_secret('hive_table')
        max_partition = context.get_secret('max_partition')

        await exporter.run(hive_table_with_schema=f'{hive_schema}.{hive_table}',
                           pg_schema=context.get_secret('postgres_schema'),
                           pg_table=context.get_secret('postgres_table'),
                           max_partition=True if max_partition is not None and max_partition.lower() == "true" else False
                           )
        context.logger.info('Data transfer finished for: {} s.'.format(datetime.now() - start_time))
    except Exception as exc:
        context.logger.error(repr(exc))
        raise exc
    finally:
        await context.on_shutdown()


if __name__ == '__main__':
    asyncio.run(main())
