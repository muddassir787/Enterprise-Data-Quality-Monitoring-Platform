import os
import sys
import argparse
import shutil
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, TimestampType, StringType

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

def main():
    parser = argparse.ArgumentParser(description="Spark Ingestion & Validation Job")
    parser.add_argument("--file-path", required=True, help="Path to raw incoming CSV file")
    args = parser.parse_args()

    file_path = args.file_path
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        sys.exit(1)

    filename = os.path.basename(file_path)
    print(f"Starting processing for file: {filename}")

    # Initialize Spark Session
    # The PostgreSQL JDBC driver jar is located at /opt/spark/jars/postgresql-42.6.0.jar
    spark = SparkSession.builder \
        .appName(f"Data-Quality-Validation-{filename}") \
        .getOrCreate()

    try:
        # 1. Read Raw CSV
        print("Reading incoming dataset into Spark DataFrame...")
        raw_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        # 2. Cast Types to match PostgreSQL Schema
        print("Casting columns to align with database types...")
        df = raw_df.withColumn("transaction_id", col("transaction_id").cast(StringType())) \
                   .withColumn("customer_id", col("customer_id").cast(StringType())) \
                   .withColumn("product_category", col("product_category").cast(StringType())) \
                   .withColumn("amount", col("amount").cast(DoubleType())) \
                   .withColumn("transaction_date", col("transaction_date").cast(TimestampType())) \
                   .withColumn("ingestion_timestamp", current_timestamp())

        # 3. Initialize Great Expectations Context
        print("Initializing Great Expectations...")
        ge_context = ge.get_context(context_root_dir="/opt/great_expectations")

        # 4. Create Batch Request for the Spark DataFrame
        batch_request = RuntimeBatchRequest(
            datasource_name="my_spark_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="sales_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
        )

        # 5. Execute Validation Checkpoint
        print("Running Great Expectations data validation suite...")
        checkpoint_result = ge_context.run_checkpoint(
            checkpoint_name="sales_data_checkpoint",
            batch_request=batch_request
        )

        # Evaluate validation results
        validation_success = checkpoint_result.list_validation_results()[0]["success"]
        print(f"Validation Success Status: {validation_success}")

        # 6. Archive and Save based on validation success
        archive_root = "/data/archive"
        success_dir = os.path.join(archive_root, "success")
        failed_dir = os.path.join(archive_root, "failed")

        os.makedirs(success_dir, exist_ok=True)
        os.makedirs(failed_dir, exist_ok=True)

        if validation_success:
            print("Validation passed. Writing data to PostgreSQL database...")
            
            db_host = os.getenv("DB_HOST", "postgres")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("WAREHOUSE_DB", "warehouse_db")
            db_user = os.getenv("WAREHOUSE_USER", "warehouse_user")
            db_pass = os.getenv("WAREHOUSE_PASSWORD", "warehouse_secure_pass")

            jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

            # Write clean data to the warehouse table
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "warehouse.sales_transactions") \
                .option("user", db_user) \
                .option("password", db_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            print("Database insert completed.")

            # Move file to success archive
            shutil.move(file_path, os.path.join(success_dir, filename))
            print(f"Source file archived to: {success_dir}")
            sys.exit(0)

        else:
            print("Validation failed! Dataset does not meet quality criteria.")
            # Move file to failed archive
            shutil.move(file_path, os.path.join(failed_dir, filename))
            print(f"Source file archived to: {failed_dir}")
            sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred during execution: {str(e)}")
        # Move file to failed archive if we failed to execute
        try:
            archive_root = "/data/archive"
            failed_dir = os.path.join(archive_root, "failed")
            os.makedirs(failed_dir, exist_ok=True)
            shutil.move(file_path, os.path.join(failed_dir, filename))
            print(f"Error handling: moved source file to: {failed_dir}")
        except Exception as move_err:
            print(f"Could not move file on error: {str(move_err)}")
        sys.exit(2)

if __name__ == "__main__":
    main()
