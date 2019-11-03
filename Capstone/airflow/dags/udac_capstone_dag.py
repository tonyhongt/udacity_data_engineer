from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTablesOperator, DataCleanOperator)
from helpers import SqlQueries




default_args = {
    'owner': 'TonyHong_Udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@monthly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_table = CreateTablesOperator(
    task_id='Create_staging_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    queries=SqlQueries.create_staging_table_queries
)

create_target_table = CreateTablesOperator(
    task_id='Create_target_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    queries=SqlQueries.create_target_table_queries
)

consolidate_operator_1 = DummyOperator(task_id='Consolidate_execution_1',  dag=dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_immigration",
    s3_bucket="udacity-capstone-hongt",
    s3_key="data/immigration/",
    region="us-west-2",
    file_format="CSV"
)


stage_city_to_redshift = StageToRedshiftOperator(
    task_id='Stage_city',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_city",
    s3_bucket="udacity-capstone-hongt",
    s3_key="reference/us-cities-demographics.csv",
    region="us-west-2",
    file_format="CSV",
    delimiter=";"
)

stage_statetemp_to_redshift = StageToRedshiftOperator(
    task_id='Stage_statetemp',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_statetemp",
    s3_bucket="udacity-capstone-hongt",
    s3_key="data/temperature/GlobalLandTemperaturesByState.csv",
    region="us-west-2",
    file_format="CSV",
    delimiter=","
)

stage_reference_to_redshift = StageToRedshiftOperator(
    task_id='Stage_reference',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_reference",
    s3_bucket="udacity-capstone-hongt",
    s3_key="reference/reference.csv",
    region="us-west-2",
    file_format="CSV",
    delimiter="|"
)


run_staging_quality_checks = DataQualityOperator(
    task_id='Run_staging_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table=['staging_reference', 'staging_city', 'staging_immigration', 'staging_statetemp']
)

run_staging_immigration_clean = DataCleanOperator(
    task_id='Clean_staging_immigration_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    sql_query=SqlQueries.staging_immigration_table_dedup
)

load_port_lookup_dimension_table = LoadDimensionOperator(
    task_id='Load_port_lookup_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="port_lookup",
    sql_query=SqlQueries.port_lookup_table_insert
)

load_country_lookup_dimension_table = LoadDimensionOperator(
    task_id='Load_country_lookup_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="country_lookup",
    sql_query=SqlQueries.country_lookup_table_insert
)

load_mode_lookup_dimension_table = LoadDimensionOperator(
    task_id='Load_mode_lookup_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="mode_lookup",
    sql_query=SqlQueries.mode_lookup_table_insert
)

load_visa_lookup_dimension_table = LoadDimensionOperator(
    task_id='Load_visa_lookup_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="visa_lookup",
    sql_query=SqlQueries.visa_lookup_table_insert
)

load_state_lookup_dimension_table = LoadDimensionOperator(
    task_id='Load_state_lookup_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="state_lookup",
    sql_query=SqlQueries.state_lookup_table_insert
)

run_dimension_quality_checks = DataQualityOperator(
    task_id='Run_dimension_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table=['port_lookup', 'country_lookup', 'mode_lookup', 'visa_lookup']
)

load_state_temperature_fact_table = LoadFactOperator(
    task_id='Load_state_temperature_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="state_temperature",
    sql_query=SqlQueries.state_temperature_table_insert
)

load_immigration_record_fact_table = LoadFactOperator(
    task_id='Load_immigration_record_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="immigration_record",
    sql_query=SqlQueries.immigration_record_table_insert
)

run_fact_quality_checks = DataQualityOperator(
    task_id='Run_fact_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table=['state_temperature', 'immigration_record']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_table
start_operator >> create_target_table

create_staging_table >> consolidate_operator_1
create_target_table >> consolidate_operator_1

consolidate_operator_1 >> stage_immigration_to_redshift
consolidate_operator_1 >> stage_city_to_redshift
consolidate_operator_1 >> stage_statetemp_to_redshift
consolidate_operator_1 >> stage_reference_to_redshift


stage_immigration_to_redshift >> run_staging_quality_checks
stage_city_to_redshift >> run_staging_quality_checks
stage_statetemp_to_redshift >> run_staging_quality_checks
stage_reference_to_redshift >> run_staging_quality_checks

run_staging_quality_checks >> run_staging_immigration_clean

run_staging_immigration_clean >> load_port_lookup_dimension_table
run_staging_immigration_clean >> load_country_lookup_dimension_table
run_staging_immigration_clean >> load_mode_lookup_dimension_table
run_staging_immigration_clean >> load_visa_lookup_dimension_table
run_staging_immigration_clean >> load_state_lookup_dimension_table

load_port_lookup_dimension_table >> run_dimension_quality_checks
load_country_lookup_dimension_table >> run_dimension_quality_checks
load_mode_lookup_dimension_table >> run_dimension_quality_checks
load_visa_lookup_dimension_table >> run_dimension_quality_checks

run_dimension_quality_checks >> load_state_temperature_fact_table
run_dimension_quality_checks >> load_immigration_record_fact_table

load_state_temperature_fact_table >> run_fact_quality_checks
load_immigration_record_fact_table >> run_fact_quality_checks

run_fact_quality_checks >> end_operator