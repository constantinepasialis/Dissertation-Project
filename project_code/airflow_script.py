from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import configparser
from google.cloud import storage
from io import StringIO

# Read config file
client = storage.Client()
bucket = client.bucket("dataops-assets")
blob = bucket.blob("config/config.ini")

config_text = blob.download_as_text()

config = configparser.ConfigParser()
config.read_string(config_text)

# Define parameters from config file
CLUSTER_NAME = config.get("cluster_details", "cluster_name")
CLUSTER_REGION = config.get("cluster_details", "location")

PROJECT_ID = config.get("project_details", "project_id")

EXTRACT_SCRIPT = config.get("scripts", "generate_extract")
TRANSFORM_SCRIPT = config.get("scripts", "transform")
MODEL_CREATION_SCRIPT = config.get("scripts", "model_creation")
LOAD_SCRIPT = config.get("scripts", "load")
LOGGER = config.get("scripts", "logger")
DB_CONNECTOR = config.get("scripts", "db_connector")

# Airflow DAG definition
default_args = {
    'owner': 'Constantinos_Pasialis',
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'email' : ["dadinos100@gmail.com"],
    'email_on_failure': False,
    'email_on_retry' : False
}

# Define the DAG
dag = DAG(
    'Data_Pipeline',
    default_args=default_args,
    description='Semen Data Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define tasks
# Task 1: Data Extraction
extract_task = DataprocSubmitJobOperator(
    task_id="Data_extraction",
    region= CLUSTER_REGION,
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": EXTRACT_SCRIPT,
            "python_file_uris": [
                LOGGER,
                DB_CONNECTOR
            ]
        },
    },
    gcp_conn_id="google_cloud_default",
    dag=dag
)

# Task 2: Data Transformation
transform_task = DataprocSubmitJobOperator(
    task_id="Data_transformation",
    region= CLUSTER_REGION,
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": TRANSFORM_SCRIPT,
            "python_file_uris": [
                LOGGER,
                DB_CONNECTOR
            ]
        },
    },
    gcp_conn_id="google_cloud_default",
    dag=dag
)

# Task 3: Model Creation
model_creation_task = DataprocSubmitJobOperator(
    task_id="Model_Creation",
    region= CLUSTER_REGION,
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": MODEL_CREATION_SCRIPT,
            "python_file_uris": [
                LOGGER,
                DB_CONNECTOR
            ]
        },
    },
    gcp_conn_id="google_cloud_default",
    dag=dag
)

# Task 4: Data Load
load_task = BashOperator(
        task_id='Data_Load',
        bash_command="""
            gcloud dataproc jobs submit pyspark "gs://europe-southwest1-datapipel-cfc3807e-bucket/scripts/pyspark_load.py" \
                --cluster=cluster1 \
                --region=europe-southwest1 \
                --jars=gs://dataops-assets/jars/mongo-spark-connector_2.12-10.4.0.jar,gs://dataops-assets/jars/mongodb-driver-sync-4.11.1.jar,gs://dataops-assets/jars/mongodb-driver-core-4.11.1.jar,gs://dataops-assets/jars/bson-4.11.1.jar \
                --py-files="gs://europe-southwest1-datapipel-cfc3807e-bucket/scripts/Logger.py,gs://europe-southwest1-datapipel-cfc3807e-bucket/scripts/DBConnector.py" \
        """
)

# Set task dependencies
extract_task >> transform_task >> model_creation_task >> load_task