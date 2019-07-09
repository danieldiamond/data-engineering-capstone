from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 test_stmt=None,
                 result=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.test_stmt = test_stmt
        self.result = result

    def execute(self, context):
        """
        Perform data quality checks on resulting fact and dimension tables.

        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
        table: string
            table located in redshift cluster
        test_stmt: string
            test SQL command to check validity of target table
        result: string
            result of test_stmt to check validity
        """
        pg_hook = PostgresHook(self.redshift_conn_id)
        records = pg_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Fail: No results for {self.table}")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Fail: 0 rows in {self.table}")

        if self.test_stmt:
            output = pg_hook.get_first(self.test_stmt)
            if self.result != output:
                raise ValueError(f"Fail: {output} != {self.result}")
        self.log.info(f"Success: {self.table} has {records[0][0]} records")
