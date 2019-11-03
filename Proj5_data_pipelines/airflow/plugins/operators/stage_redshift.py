from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        IGNOREHEADER {}
        {} '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 ignore_headers=1,
                 file_format="JSON",
                 json_path="",
                 file_dated=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region= region
        self.ignore_headers = ignore_headers
        self.file_format = file_format
        self.json_path = json_path
        self.file_dated = file_dated
        self.execution_date = kwargs.get('ds')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Running data load for {}'.format(self.execution_date))
        
        self.log.info('Clearing data from target tables')
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        

        self.log.info('Load data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info('Lets see...{}'.format(s3_path))
        
        if self.json_path == "":
            js_path = "auto"
        else:
            js_path = "s3://{}/".format(self.s3_bucket) + self.json_path        
                     
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.ignore_headers,
            self.file_format,
            js_path
        )
        
        redshift.run(formatted_sql)






