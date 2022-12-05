"""
pip install pytest-mock

pytest -s test_mappings.py
"""
from sqoop.export_to_postgres import ExportToPostgres


class FakeSparkDf:
    def __init__(self, dtypes):
        self._dtypes = dtypes

    @property
    def dtypes(self):
        return self._dtypes

    @dtypes.setter
    def dtypes(self, value):
        self._dtypes = value

    def withColumn(self, *args, **kwargs):
        return self


class FakeAttr:
    @staticmethod
    def cast(*args, **kwargs):
        pass


def test_mapping(mocker):
    test_dtypes = [('inn', 'string'), ('mon', 'string'), ('acc_date_op', 'timestamp'), ('acc_id', 'string'),
                   ('acc_num', 'string'),
                   ('active_flag', 'int'), ('active_flag_end_mon', 'int'), ('avg_fl_age', 'int'),
                   ('avg_fl_age_group', 'string')]

    spark_df = FakeSparkDf(test_dtypes)
    mocker.patch('export_to_postgres.col', return_value=FakeAttr)

    # Test user types mapping
    user_types_mapping = ('inn: int', 'mon: timestamp', 'acc_date_op:decimal(38,18)',
                          'avg_fl_age_group :   bigint', 'active_flag: bool', 'avg_fl_age: text')
    spark_df, new_types = ExportToPostgres.mapping_types(spark_df=spark_df, user_types_mapping=user_types_mapping)
    assert new_types.split(', ') == ['inn int', 'mon timestamp', 'acc_date_op decimal(38,18)',
                                     'active_flag bool', 'active_flag_end_mon integer', 'avg_fl_age_group bigint']

    # Test 'text' type in user types mapping
    user_types_mapping = ('acc_date_op: text', 'active_flag: text', 'avg_fl_age: decimal(38,18)')
    spark_df, new_types = ExportToPostgres.mapping_types(spark_df=spark_df, user_types_mapping=user_types_mapping)
    assert new_types.split(', ') == ['active_flag_end_mon integer', 'avg_fl_age decimal(38,18)']

    # Test INT types mapping
    # user_types_mapping = ('inn: smallint', 'mon: int', 'acc_date_op: integer', 'acc_id: bigint')
    test_dtypes = [('inn', 'smallint'), ('mon', 'int'), ('acc_date_op', 'integer'), ('acc_id', 'bigint'),
                   ('acc_num', 'tinyint'),
                   ('active_flag', 'float'), ('active_flag_end_mon', 'numeric'), ('avg_fl_age', 'double'),
                   ('avg_fl_age_group', 'double precision'), ('branch_cnt', 'decimal(38, 18)')
                   ]
    spark_df = FakeSparkDf(test_dtypes)
    spark_df, new_types = ExportToPostgres.mapping_types(spark_df=spark_df, user_types_mapping=())
    assert new_types.split(', ') == ['inn integer', 'mon integer', 'acc_date_op integer', 'acc_id bigint',
                                     'active_flag float', 'active_flag_end_mon float', 'avg_fl_age float',
                                     'avg_fl_age_group float', 'branch_cnt float']

    # Test unknown type
    test_dtypes = [('inn', 'array[string]'), ('mon', 'srray'), ('acc_date_op', 'timestamp'), ('acc_id', 'bigint'),
                   ('acc_num', 'tinyint'),
                   ('active_flag', 'text'), ('active_flag_end_mon', 'text'),
                   ('avg_fl_age_group', 'double precision'), ('branch_cnt', 'decimal(38, 18)')
                   ]
    spark_df = FakeSparkDf(test_dtypes)
    spark_df, new_types = ExportToPostgres.mapping_types(spark_df=spark_df, user_types_mapping=())
    assert new_types.split(', ') == ['acc_id bigint', 'avg_fl_age_group float', 'branch_cnt float']


# async def main():
#     import os
#     from dotenv import load_dotenv
#
#     load_dotenv()
#     context = AppContext()
#     await context.start_asyncpg_pool(pg_dsn=os.getenv('PG_PATRONI'))
#
#     await create_backup(context, 'custom_cib_ml360', 'mon_basis_corporate')
    # await drop_tbl_if_exists(context, 'custom_cib_ml360.backup_mon_smart_features')

    # await change_partitions(context, 'exporter_tmp', 'tmp_mon_basis_corporate', 'mon_basis_corporate')

    # await change_partitions(context, 'custom_cib_ml360', 'mon_basis_corporate')
    # await update_table_privileges(context, 'custom_cib_ml360', 'mon_basis_corporate')
    # await add_function(context, 'exporter_tmp')
    # await context.on_shutdown()


# if __name__ == '__main__':
#     import asyncio
#
#     asyncio.run(main())