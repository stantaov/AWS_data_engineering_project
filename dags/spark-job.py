
import os
import boto3
import logging
import airflow
from airflow import DAG
import utils.emr_utils as emr
from datetime import timedelta
from airflow.models import Variable
from airflow import AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.custom_plugin import ClusterCheckSensor


DEFAULT_ARGS = {
    'owner': 'Stan Taov',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1), # enable event driven approach
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

# Retrives filename and file extension of the uploaded file
def file_extension_checker(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    base=os.path.basename(s3_location)
    file_name, extension = os.path.splitext(base)
    extension = extension[1:]
    Variable.set("extension", extension.capitalize())
    Variable.set("file_name", file_name)
    return extension.capitalize()

# Retrives location of the uploaded file on S3
def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key = 's3_location', value = s3_location)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region)
    Variable.set("cluster_id", cluster_id)
    return cluster_id

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)
    # Sets Airflow Variable key for cluster_id to na
    Variable.set("cluster_id", "na")

# ELT job complete pointer
def dag_done(**kwargs):
    Variable.set("dag_emr_job", "done")

# Runs predefined Glue crawler 
def cwarler_run(**kwargs):
    client = boto3.client('glue', region_name=region)
    response = client.start_crawler(
    Name='data_enginnering_midterm_project'
    )

# Repairs Athena partitions
def reapir_table(**kwargs):
    client = boto3.client('athena', region_name=region)
    queryStart = client.start_query_execution(
        QueryString = "MSCK REPAIR TABLE " + Variable.get("file_name"),
        QueryExecutionContext = {
        'Database': 'data_enginnering_midterm_project'
        }, 
        ResultConfiguration = { 'OutputLocation': 's3://midterm-project-wcd/output_data/'}
    )

region = emr.get_region()
cluster = emr.emr_clent(region_name=region)

SPARK_STEPS = [
    {
        'Name': 'spark-job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','1g',
                '--executor-memory','3g',
                '--executor-cores','2',
                '--conf', 'spark.shuffle.service.enabled=true',
                '--conf', 'spark.dynamicAllocation.enabled=true',
                '--conf', 'spark.dynamicAllocation.minExecutors=2',
                '--conf', 'spark.dynamicAllocation.maxExecutors=10',
                '--conf', 'spark.dynamicAllocation.initialExecutors=2',
                's3a://midterm-project-wcd/spark-job/spark-engine_2.12-0.0.1-spark3.jar',
                '-p','midterm-project',
                '-i', Variable.get("extension", default_var=0), 
                '-o','parquet',
                #'-s','s3a://midterm-project-wcd/input_data/banking.csv',
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3_location') }}",
                '-d','s3a://midterm-project-wcd/output_data/' + Variable.get("file_name", default_var=0) + '/',
                '-c','year', # come up with a solution to pick partition automatically
                '-m','append',
                '--input-options','header=true, inferSchema=true' # header=true, inferSchema=true
            ]
        }
    }
]


# Creating DAG workflow 
dag = DAG(
    'emr_spark_job',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=['emr']
)

get_extension = PythonOperator(
    task_id='get_extension',
    python_callable=file_extension_checker,
    dag=dag)

create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

emr_cluster_check = ClusterCheckSensor(
    task_id='cluster_check', 
    poke_interval=60, 
    dag=dag)

parse_request = PythonOperator(
    task_id='parse_request',
    provide_context=True,
    python_callable=retrieve_s3_file,
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

crawler = PythonOperator(
    task_id = 'glue_crawler',
    python_callable=cwarler_run,
    dag=dag
)

repair = PythonOperator(
    task_id = 'rapair_athena_table',
    python_callable=reapir_table,
    dag=dag
)

job_complete = PythonOperator(
    task_id = "ETL_job_complete",
    python_callable=dag_done,
    dag=dag
)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_success', # make sure that all tasks were completed successfully
    dag=dag)

# Setting the Airflow workflow
get_extension >> parse_request
parse_request >> create_cluster
create_cluster >> emr_cluster_check
emr_cluster_check >> step_adder
step_adder >> step_checker
step_checker >> crawler
crawler >> repair
repair >> job_complete
job_complete >> terminate_cluster