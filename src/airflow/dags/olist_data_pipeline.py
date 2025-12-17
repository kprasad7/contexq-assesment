"""Airflow DAG for Olist end-to-end orchestration.

Terraform provisions the AWS Glue jobs; this DAG only triggers them and passes
runtime args.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


AWS_CONN_ID = Variable.get("aws_conn_id", default_var="aws_default")
AWS_REGION = Variable.get("aws_region", default_var="us-east-1")

RAW_DATA_BUCKET = Variable.get("raw_data_bucket", default_var="__REQUIRED__")
PROCESSED_DATA_BUCKET = Variable.get("processed_data_bucket", default_var="__REQUIRED__")
GLUE_DATABASE = Variable.get("glue_database", default_var="contexq_dev")
CORPORATE_REGISTRY_TABLE = Variable.get(
    "corporate_registry_table",
    default_var="corporate_registry",
)

ETL_JOB_NAME = Variable.get("glue_etl_job_name", default_var="contexq-dev-etl-job")
ML_JOB_NAME = Variable.get(
    "glue_ml_training_job_name",
    default_var="contexq-dev-ml-training",
)

MLFLOW_TRACKING_URI = Variable.get(
    "mlflow_tracking_uri",
    default_var="http://localhost:5000",
)
MLFLOW_EXPERIMENT_NAME = Variable.get(
    "mlflow_experiment_name",
    default_var="olist-profit-prediction",
)


with DAG(
    dag_id="olist_data_pipeline",
    default_args=default_args,
    description="End-to-end Olist Data & AI pipeline (Glue jobs)",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["olist", "etl", "ml", "glue"],
) as dag:
    run_etl = AwsGlueJobOperator(
        task_id="run_etl",
        job_name=ETL_JOB_NAME,
        script_args={
            "--source_bucket": RAW_DATA_BUCKET,
            "--target_bucket": PROCESSED_DATA_BUCKET,
            "--database": GLUE_DATABASE,
            "--table": CORPORATE_REGISTRY_TABLE,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    run_ml_training = AwsGlueJobOperator(
        task_id="run_ml_training",
        job_name=ML_JOB_NAME,
        script_args={
            "--database": GLUE_DATABASE,
            "--table": CORPORATE_REGISTRY_TABLE,
            "--mlflow_tracking_uri": MLFLOW_TRACKING_URI,
            "--experiment_name": MLFLOW_EXPERIMENT_NAME,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    run_etl >> run_ml_training
