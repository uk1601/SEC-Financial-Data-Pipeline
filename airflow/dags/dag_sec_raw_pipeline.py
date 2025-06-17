#!/usr/bin/env python3
"""
Apache Airflow DAG: sec_raw_data_to_snowflake

Extracts raw SEC data, transforms to CSV/Parquet, uploads to S3,
creates Snowflake stage, file formats, tables, and loads raw data.
"""
import logging
from io import BytesIO
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_csv import csv_transformer
from sec_scraper.convert_to_parquet import parquet_transformer

# Configure module-level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Airflow Variables for S3 and AWS credentials
BUCKET_NAME = Variable.get("s3_bucket_name")
AWS_KEY_ID = Variable.get("AWS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
S3_URI = f"s3://{BUCKET_NAME}"

# Default arguments for all tasks in this DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}


def run_scraper_and_transform(**kwargs) -> None:
    """
    Scrape SEC data, transform to CSV and Parquet, then upload to S3.

    Pulls `source`, `year`, and `quarter` from DagRun configuration.
    Pushes `year` and `quarter` to XCom for downstream tasks.
    """
    ti = kwargs['ti']
    conf = kwargs['dag_run'].conf or {}
    source = conf.get('source')
    year = conf.get('year')
    quarter = int(conf.get('quarter').lstrip('Q'))

    logger.info(f"Starting scrape for {source}, Year={year}, Quarter={quarter}")
    extracted = scrape_sec_data(year, quarter)
    logger.info("Scrape completed, transforming files...")

    # Transform to CSV and Parquet
    csv_files = csv_transformer(extracted, year, quarter)
    parquet_files = parquet_transformer(extracted, year, quarter)
    all_files = csv_files + parquet_files

    # Upload each transformed file to S3
    hook = S3Hook(aws_conn_id='aws_default')
    for filename, buffer in all_files:
        buffer.seek(0)
        file_type = filename.rsplit('.', 1)[1]
        key = f"data/{year}/{quarter}/{file_type}/{filename}"
        hook.load_file_obj(
            file_obj=buffer,
            bucket_name=BUCKET_NAME,
            key=key,
            replace=True
        )
        logger.info(f"Uploaded {filename} to s3://{BUCKET_NAME}/{key}")

    # Push metadata for downstream schema and copy tasks
    ti.xcom_push(key='year', value=year)
    ti.xcom_push(key='quarter', value=quarter)
    logger.info("All transformed files uploaded to S3.")


def make_schema(table: str, columns: str, **kwargs) -> str:
    """
    Dynamically generate CREATE TABLE statement for raw table.

    Args:
        table: Base table name (sub, num, tag, pre)
        columns: Comma-separated column definitions
    Returns:
        SQL string to create or replace the table.
    """
    ti = kwargs['ti']
    year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
    quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
    table_name = f"{table}_{year}_Q{quarter}"
    return f"""
        CREATE OR REPLACE TABLE {table_name} (
            {columns},
            year INT,
            quarter INT
        );
    """


def make_copy(table: str, **kwargs) -> str:
    """
    Generate COPY INTO statement to load CSV data from S3 into Snowflake.

    Args:
        table: Base table name (sub, num, tag, pre)
    Returns:
        SQL string for COPY INTO command.
    """
    ti = kwargs['ti']
    year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
    quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
    table_name = f"{table}_{year}_Q{quarter}"
    return f"""
        COPY INTO {table_name}
        FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/{table}.csv
        FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
    """


# Define the main DAG context
with DAG(
    dag_id='sec_raw_data_to_snowflake',
    default_args=default_args,
    description='Extract SEC data, transform and load raw tables to Snowflake',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sec_datapipelines']
) as dag:

    # Task: Scrape and upload raw files
    scrape_task = PythonOperator(
        task_id='scrape_sec_data',
        python_callable=run_scraper_and_transform,
        provide_context=True
    )

    # Task: Create S3 stage in Snowflake
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_v2',
        sql=f"""
            USE ROLE dbt_role;
            CREATE OR REPLACE STAGE sec_s3_stage_csv
            URL = '{S3_URI}'
            CREDENTIALS = (
                AWS_KEY_ID = '{AWS_KEY_ID}',
                AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
            );
        """
    )

    # Task: Create CSV file format
    create_fileformat_csv = SnowflakeOperator(
        task_id='create_fileformat_csv',
        snowflake_conn_id='snowflake_v2',
        sql="""
            CREATE OR REPLACE FILE FORMAT csv_fileformat
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            NULL_IF = ('', 'NULL')
            PARSE_HEADER = TRUE
            DATE_FORMAT = 'YYYYMMDD'
            EMPTY_FIELD_AS_NULL = TRUE
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
        """
    )

    # List of raw tables and their column definitions
    raw_tables = {
        'sub': """
            adsh VARCHAR PRIMARY KEY,
            cik VARCHAR,
            name VARCHAR,
            sic VARCHAR,
            countryba VARCHAR,
            stprba VARCHAR,
            cityba VARCHAR,
            zipba VARCHAR,
            bas1 VARCHAR,
            bas2 VARCHAR,
            baph VARCHAR,
            countryma VARCHAR,
            stprma VARCHAR,
            cityma VARCHAR,
            zipma VARCHAR,
            mas1 VARCHAR,
            mas2 VARCHAR,
            countryinc VARCHAR,
            stprinc VARCHAR,
            ein VARCHAR,
            former VARCHAR,
            changed VARCHAR,
            afs VARCHAR,
            wksi VARCHAR,
            fye VARCHAR,
            form VARCHAR,
            period VARCHAR,
            fy VARCHAR,
            fp VARCHAR,
            filed VARCHAR,
            accepted VARCHAR,
            prevrpt VARCHAR,
            detail VARCHAR,
            instance VARCHAR,
            nciks VARCHAR,
            aciks VARCHAR NULL
        """,
        'num': """
            adsh VARCHAR PRIMARY KEY,
            tag VARCHAR,
            version VARCHAR,
            ddate VARCHAR,
            qtrs VARCHAR,
            uom VARCHAR,
            segments VARCHAR,
            coreg VARCHAR,
            value VARCHAR,
            footnote VARCHAR
        """,
        'tag': """
            tag VARCHAR,
            version VARCHAR,
            custom VARCHAR,
            abstract VARCHAR,
            datatype VARCHAR,
            iord VARCHAR,
            crdr VARCHAR,
            tlabel VARCHAR,
            doc VARCHAR
        """,
        'pre': """
            adsh VARCHAR PRIMARY KEY,
            report VARCHAR,
            line VARCHAR,
            stmt VARCHAR,
            inpth VARCHAR,
            rfile VARCHAR,
            tag VARCHAR,
            version VARCHAR,
            plabel VARCHAR,
            negating VARCHAR
        """
    }

    # Dynamically create schema and load tasks for each raw table: num,pre,sub,tag
    for tbl, cols in raw_tables.items():
        create_schema = PythonOperator(
            task_id=f'generate_schema_sql_{tbl}',
            python_callable=make_schema,
            op_kwargs={'table': tbl, 'columns': cols},
            provide_context=True
        )

        schema_task = SnowflakeOperator(
            task_id=f'schema_creation_{tbl}',
            snowflake_conn_id='snowflake_v2',
            sql="{{ ti.xcom_pull(task_ids='generate_schema_sql_" + tbl + "') }}"
        )

        create_copy = PythonOperator(
            task_id=f'generate_copy_sql_{tbl}',
            python_callable=make_copy,
            op_kwargs={'table': tbl},
            provide_context=True
        )

        copy_task = SnowflakeOperator(
            task_id=f'copy_from_s3_{tbl}',
            snowflake_conn_id='snowflake_v2',
            sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_" + tbl + "') }}"
        )

        # Set task dependencies for this table
        scrape_task >> create_stage >> create_fileformat_csv >> create_schema >> schema_task >> create_copy >> copy_task
