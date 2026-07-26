import os
import glob
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "data_quality_pipeline",
    default_args=default_args,
    description="Enterprise Data Quality Monitoring Pipeline",
    schedule_interval=None,  # Configured to run on-demand or externally triggered
    catchup=False,
    tags=["data-quality", "great-expectations", "spark"],
) as dag:

    @task
    def scan_incoming_files():
        """
        Scans the shared /data/incoming folder for CSV datasets.
        Returns a list of argument arrays for the SparkSubmitOperator.
        """
        incoming_pattern = "/data/incoming/*.csv"
        files = glob.glob(incoming_pattern)
        
        if not files:
            print("No new incoming CSV files found.")
            return []
            
        print(f"Found {len(files)} new files to validate:")
        for f in files:
            print(f" - {f}")
            
        # Format the arguments as required by SparkSubmitOperator.expand()
        # Each entry will be mapped to a separate dynamic task execution
        return [["--file-path", f] for f in files]

    # Map SparkSubmitOperator dynamically across all files found
    # This executes validation concurrently on the Spark cluster
    run_spark_validation = SparkSubmitOperator.partial(
        task_id="validate_file",
        application="/opt/spark/jobs/validate_and_process.py",
        conn_id="spark_default",
        verbose=True,
    ).expand(application_args=scan_incoming_files())
