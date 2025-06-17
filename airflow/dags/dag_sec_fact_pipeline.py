#!/usr/bin/env python3
"""
Airflow DAG: sec_fact_data_to_snowflake

Extracts SEC data, transforms to CSV/Parquet, loads raw tables into Snowflake,
then runs dbt to build and test fact models.
"""
import logging
from io import BytesIO
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_csv import csv_transformer
from sec_scraper.convert_to_parquet import parquet_transformer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Airflow variables
BUCKET_NAME = Variable.get("s3_bucket_name")
AWS_KEY_ID = Variable.get("AWS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
S3_URI = f"s3://{BUCKET_NAME}"

# DBT directories
DBT_PROFILE_DIR = "/opt/airflow/data_pipeline/profiles"
DBT_PROJECT_DIR = "/opt/airflow/data_pipeline"

# Default task arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}


def run_scraper_and_transform(**kwargs) -> None:
    """
    Scrape SEC data for given year/quarter, transform to CSV/Parquet,
    and upload all files to S3.
    Pushes 'year' and 'quarter' to XCom.
    """
    ti = kwargs['ti']
    conf = kwargs['dag_run'].conf or {}
    source = conf.get('source')
    year = conf.get('year')
    quarter = int(conf.get('quarter').lstrip('Q'))

    logger.info(f"Scraping SEC data: {source} Year={year} Q{quarter}")
    extracted = scrape_sec_data(year, quarter)
    logger.info("Converting to CSV and Parquet...")
    csv_files = csv_transformer(extracted, year, quarter)
    parquet_files = parquet_transformer(extracted, year, quarter)
    all_files = csv_files + parquet_files

    s3 = S3Hook(aws_conn_id='aws_default')
    for fname, buf in all_files:
        buf.seek(0)
        file_type = fname.rsplit('.',1)[1]
        key = f"data/{year}/{quarter}/{file_type}/{fname}"
        s3.load_file_obj(file_obj=buf, bucket_name=BUCKET_NAME, key=key, replace=True)
        logger.info(f"Uploaded {fname} to s3://{BUCKET_NAME}/{key}")

    ti.xcom_push(key='year', value=year)
    ti.xcom_push(key='quarter', value=quarter)
    logger.info("All files uploaded to S3.")


def make_schema_and_copy(table: str, columns: str, **kwargs) -> None:
    """
    Create raw table in Snowflake and copy CSV data from S3.

    Args:
        table: base name (sub, num, pre, tag)
        columns: comma-separated column definitions
    """
    ti = kwargs['ti']
    year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
    quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
    table_name = f"raw_{table}_{year}_Q{quarter}"

    create_sql = f"""
        CREATE OR REPLACE TABLE {table_name} (
            {columns},
            year INT,
            quarter INT
        );
    """
    copy_sql = f"""
        COPY INTO {table_name}
        FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/{table}.csv
        FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
    """
    # Execute via xcom
    ti.xcom_push(key=f'sql_schema_{table}', value=create_sql)
    ti.xcom_push(key=f'sql_copy_{table}', value=copy_sql)


def run_dbt(task: str) -> BashOperator:
    """
    Return a BashOperator for dbt run/test.
    """
    cmd = f"cd {DBT_PROJECT_DIR} && dbt {task} --profiles-dir {DBT_PROFILE_DIR}"
    return BashOperator(
        task_id=f'dbt_{task}',
        bash_command=cmd
    )

# Define DAG
with DAG(
    dag_id='sec_fact_data_to_snowflake',
    start_date=datetime(2025,2,1),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['sec_datapipelines'],
    description='Load raw tables then run dbt for fact models'
) as dag:

    scrape = PythonOperator(
        task_id='scrape_sec_data',
        python_callable=run_scraper_and_transform,
        provide_context=True
    )

    # Stage and file format creation
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_v2',
        sql=f"""
            USE ROLE dbt_role;
            CREATE OR REPLACE STAGE sec_s3_stage_csv
            URL='{S3_URI}'
            CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}',AWS_SECRET_KEY='{AWS_SECRET_KEY}');
        """
    )
    create_format = SnowflakeOperator(
        task_id='create_fileformat_csv',
        snowflake_conn_id='snowflake_v2',
        sql="""
            CREATE OR REPLACE FILE FORMAT csv_fileformat
            TYPE=CSV
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            NULL_IF=('','NULL')
            PARSE_HEADER=TRUE
            DATE_FORMAT='YYYYMMDD'
            EMPTY_FIELD_AS_NULL=TRUE
            TRIM_SPACE=TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;
        """
    )

    # Raw table definitions and copy
    raw_defs = {
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

    # Tasks for each raw model
    schema_tasks = []
    for tbl, cols in raw_defs.items():
        gen_schema = PythonOperator(
            task_id=f'generate_schema_{tbl}',
            python_callable=make_schema_and_copy,
            op_kwargs={'table': tbl, 'columns': cols},
            provide_context=True
        )
        create_tbl = SnowflakeOperator(
            task_id=f'create_table_{tbl}',
            snowflake_conn_id='snowflake_v2',
            sql="{{ ti.xcom_pull(task_ids='generate_schema_" + tbl + "', key='sql_schema_" + tbl + "') }}"
        )
        copy_tbl = SnowflakeOperator(
            task_id=f'copy_table_{tbl}',
            snowflake_conn_id='snowflake_v2',
            sql="{{ ti.xcom_pull(task_ids='generate_schema_" + tbl + "', key='sql_copy_" + tbl + "') }}"
        )
        schema_tasks.append((gen_schema, create_tbl, copy_tbl))

    # dbt tasks
    dbt_run = run_dbt('run')
    dbt_test = run_dbt('test')

    # Set dependencies
    scrape >> create_stage >> create_format
    for gen, create_tbl, copy_tbl in schema_tasks:
        create_format >> gen >> create_tbl >> copy_tbl >> dbt_run
    dbt_run >> dbt_test
