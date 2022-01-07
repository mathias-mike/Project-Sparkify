from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                et_query="", # et: Extract Transform   -   Load happens below
                insert_mode="INSERT",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.et_query = et_query
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "TRUNCATE_INSERT":
            self.log.info('Clearing all data from destination table')
            redshift.run(f"TRUNCATE TABLE {self.table}")

        redshift.run(f"""
            INSERT INTO {self.table}
            {self.et_query}
        """)
        self.log.info(f'Load {self.table} success!')

