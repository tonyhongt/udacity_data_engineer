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
        {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_format="CSV",
                 delimiter=",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region= region
        self.file_format = file_format
        self.execution_date = kwargs.get('ds')
        self.delimiter = delimiter

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
        
        additional=""
        if self.file_format == 'CSV':
            additional = "TIMEFORMAT as 'epochmillisecs' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1"
            if self.delimiter != ',':
                additional = additional + " DELIMITER '" + self.delimiter + "'"    
                     
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format,
            additional
        )
        """
        formatted_sql = "COPY staging_city\
        FROM 's3://udacity-capstone-hongt/sample/us-cities-demographics.csv'\
        ACCESS_KEY_ID 'AKIAQ4G7FNFBWDY7K4IQ'\
        SECRET_ACCESS_KEY 'tcTW6PhOkbfBg2+3k61/UBGApIFtAV++DjkJL0jP'\
        TIMEFORMAT as 'epochmillisecs'\
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL\
        csv\
        DELIMITER ';' IGNOREHEADER 1"
        """
        redshift.run(formatted_sql)






