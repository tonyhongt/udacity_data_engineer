from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.create_table_queries import create_table_queries, drop_table_queries

class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Start dropping existing Redshift tables: ")
        for query in drop_table_queries:
            redshift.run(query)
        
        self.log.info("Creating Redshift tables: ")
        for query in create_table_queries:
            redshift.run(query)        

        
