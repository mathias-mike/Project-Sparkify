from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check.get('test_query')
            expected_result = check.get('expected_result')

            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. \n{sql}\n returned no results!")
            
            test_result = records[0][0]
            if test_result != expected_result:
                raise ValueError(f"""Data quality check failed "X". 
                                \n{sql}\nreturned: {test_result}\nexpected: {expected_result}""")
        
        self.log.info('Data quality check passed "✓"')


