from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {table_name}
        FROM '{s3_location}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        JSON '{json_load_option}'
    """

    @apply_defaults
    def __init__(self,
                table="",
                s3_bucket="",
                s3_key="",
                json_load_option="auto ignorecase",
                redshift_conn_id="",
                aws_credentials_id="",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket   
        self.s3_key = s3_key
        self.json_load_option = json_load_option
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id     

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        self.log.info('Clearing all data from destination table')
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f'Stage data into {self.table}')
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        redshift.run(StageToRedshiftOperator.copy_sql.format(
            table_name = self.table,
            s3_location = s3_path,
            access_key_id = credentials.access_key,
            secret_access_key = credentials.secret_key,
            json_load_option = self.json_load_option
        ))

        self.log.info(f'Stagging into {self.table} success!')




