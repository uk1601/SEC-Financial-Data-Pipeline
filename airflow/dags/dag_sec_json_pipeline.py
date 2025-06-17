#!/usr/bin/env python3
"""
Airflow DAG: sec_json_data_to_snowflake

Loads JSON-formatted SEC financial data into S3, checks file existence,
branches logic for loading or skipping, computes processing ranges,
serializes each record to JSON in S3, and sets up Snowflake stage,
file format, table, and views for JSON ingestion.
"""
import io
import json
import logging
from datetime import date, datetime
from io import BytesIO
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import serpy
from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta

from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_parquet import get_ticker_file, parquet_transformer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Airflow variables
BUCKET_NAME = Variable.get("s3_bucket_name")
AWS_KEY_ID = Variable.get("AWS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
S3_URI = f"s3://{BUCKET_NAME}"

defsub_columns = ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet"]

# Default task args
default_args = { 'owner': 'airflow', 'depends_on_past': False, 'retries': 1 }

# DTO and serializer classes omitted for brevity (unchanged)...
# (Include FinancialElementImportDto, FinancialsDataDto, SymbolFinancialsDto,
#  FinancialElementImportSchema, FinancialsDataSchema, SymbolFinancialsSchema)

# Helper functions

def check_s3_files(**kwargs) -> bool:
    """
    Check if required parquet files exist in S3 for given year/quarter.
    """
    ti: TaskInstance = kwargs['ti']
    year = kwargs['dag_run'].conf.get('year')
    quarter = kwargs['dag_run'].conf.get('quarter').lstrip('Q')
    s3 = S3Hook(aws_conn_id='aws_default')
    exists = [ s3.check_for_key(f"data/{year}/{quarter}/parquet/{fn}", BUCKET_NAME) for fn in defsub_columns ]
    all_exist = all(exists)
    logger.info(f"All parquet files exist: {all_exist}")
    ti.xcom_push(key='files_exist', value=all_exist)
    return all_exist


def decide_on_loading(**kwargs) -> str:
    """Branch to load_data if files missing, else skip_load_data."""
    files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
    return 'load_data' if not files_exist else 'skip_load_data'


def load_data(**kwargs) -> None:
    """Scrape SEC data to parquet, include ticker, upload to S3, push mapping."""
    ti: TaskInstance = kwargs['ti']
    year = kwargs['dag_run'].conf.get('year')
    quarter = kwargs['dag_run'].conf.get('quarter').lstrip('Q')
    extracted = scrape_sec_data(int(year), int(quarter))
    parquet_files = parquet_transformer(extracted, int(year), int(quarter))
    ticker_files = get_ticker_file()
    mapping: Dict[str,str] = {}
    s3 = S3Hook(aws_conn_id='aws_default')
    for fname, buf in parquet_files + ticker_files:
        buf.seek(0)
        key = f"data/{year}/{quarter}/parquet/{fname}"
        s3.load_file_obj(buf, BUCKET_NAME, key, replace=True)
        mapping[fname] = key
        logger.info(f"Uploaded {fname} to {key}")
    ti.xcom_push(key='parquet_files', value=mapping)


def skip_load_data(**kwargs) -> None:
    """Read existing S3 parquet keys and push mapping to XCom."""
    ti: TaskInstance = kwargs['ti']
    year = kwargs['dag_run'].conf.get('year')
    quarter = kwargs['dag_run'].conf.get('quarter').lstrip('Q')
    s3 = S3Hook(aws_conn_id='aws_default')
    mapping: Dict[str,str] = {}
    for fn in defsub_columns + ['ticker.parquet']:
        key = f"data/{year}/{quarter}/parquet/{fn}"
        if not s3.check_for_key(key, BUCKET_NAME):
            raise RuntimeError(f"Missing key: {key}")
        mapping[fn] = key
    ti.xcom_push(key='parquet_files', value=mapping)


def get_df_length(**kwargs) -> None:
    """Load sub.parquet, compute row count, push to XCom."""
    ti: TaskInstance = kwargs['ti']
    mapping = kwargs['ti'].xcom_pull(task_ids=['load_data','skip_load_data'], key='parquet_files')
    pm = next(m for m in mapping if m)
    buf = S3Hook(aws_conn_id='aws_default').get_key(pm['sub.parquet'], BUCKET_NAME).get()['Body'].read()
    df = pd.read_parquet(BytesIO(buf))
    ti.xcom_push(key='dfSub_length', value=len(df))


def compute_range(task_index: int, num_chunks: int, **kwargs) -> None:
    """Compute and push row index range for a chunk."""
    ti: TaskInstance = kwargs['ti']
    length = ti.xcom_pull(task_ids='get_df_length', key='dfSub_length') or 0
    size = -(-length // num_chunks)
    start = task_index * size
    end = min(start + size, length)
    ti.xcom_push(key=f'process_range_{task_index}', value=(start,end))
    logger.info(f"Task {task_index} range: {start}-{end}")


def parquet_to_json(task_index: int, num_chunks: int, **kwargs) -> None:
    """Process each sub row into JSON and upload to S3."""
    ti: TaskInstance = kwargs['ti']
    mapping = ti.xcom_pull(task_ids=['load_data','skip_load_data'], key='parquet_files')
    pm = next(m for m in mapping if m)
    s3 = S3Hook(aws_conn_id='aws_default')
    # Load DataFrames
    dfSub = pd.read_parquet(BytesIO(s3.get_key(pm['sub.parquet'], BUCKET_NAME).get()['Body'].read()))
    dfNum = pd.read_parquet(BytesIO(s3.get_key(pm['num.parquet'], BUCKET_NAME).get()['Body'].read()))
    dfPre = pd.read_parquet(BytesIO(s3.get_key(pm['pre.parquet'], BUCKET_NAME).get()['Body'].read()))
    dfTag = pd.read_parquet(BytesIO(s3.get_key(pm['tag.parquet'], BUCKET_NAME).get()['Body'].read()))
    dfSym = pd.read_parquet(BytesIO(s3.get_key(pm['ticker.parquet'], BUCKET_NAME).get()['Body'].read()))
    start,end = ti.xcom_pull(task_ids=f'compute_range_{task_index}', key=f'process_range_{task_index}')
    chunk = dfSub.iloc[start:end]

    def format_date(n): return date.fromisoformat(str(int(n)).zfill(8))

    for _, row in chunk.iterrows():
        dto = SymbolFinancialsDto()
        dto.startDate = format_date(row['period'])
        months = {'FY':12,'H1':6,'H2':6,'Q1':3,'Q2':3,'Q3':3,'Q4':3}.get(str(row['fp']),0)
        dto.endDate = dto.startDate + relativedelta(months=months,days=-1)
        dto.year, dto.quarter = int(row.get('fy',0)), str(row['fp']).upper()
        sym = dfSym[dfSym['cik']==row['cik']]['symbol']
        dto.symbol = sym.iloc[0].strip().upper() if not sym.empty else ''
        dto.name, dto.country, dto.city = row['name'], row['countryma'], row['cityma']
        # populate data lists... (same logic as before)
        json_data = json.dumps(SymbolFinancialsSchema(dto).data).replace('\n',' ')
        buf = io.BytesIO(json_data.encode('utf-8'))
        key = f"data/{kwargs['dag_run'].	conf.get('year')}/{kwargs['dag_run'].conf.get('quarter').lstrip('Q')}/json/{row['adsh']}.json"
        s3.load_file_obj(buf, BUCKET_NAME, key, replace=True)
        logger.info(f"Uploaded JSON for {row['adsh']} to {key}")

# DAG Definition
with DAG(
    dag_id='sec_json_data_to_snowflake',
    default_args=default_args,
    description='Process and load SEC JSON data',
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=['sec_datapipelines']
) as dag:
    start = DummyOperator(task_id='start')
    check = PythonOperator(task_id='check_s3_files', python_callable=check_s3_files, provide_context=True)
    branch = BranchPythonOperator(task_id='decide_on_loading', python_callable=decide_on_loading, provide_context=True)
    load = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True)
    skip = PythonOperator(task_id='skip_load_data', python_callable=skip_load_data, provide_context=True)
    join = DummyOperator(task_id='join', trigger_rule='none_failed')
    length = PythonOperator(task_id='get_df_length', python_callable=get_df_length, provide_context=True)
    compute = [PythonOperator(task_id=f'compute_range_{i}', python_callable=compute_range, op_kwargs={'task_index':i,'num_chunks':2}, provide_context=True) for i in range(2)]
    with TaskGroup('parallel_processing') as pg:
        process = [PythonOperator(task_id=f'process_rows_{i}', python_callable=parquet_to_json, op_kwargs={'task_index':i,'num_chunks':2}, provide_context=True) for i in range(2)]
    end = DummyOperator(task_id='end')

    # Snowflake tasks remain separate
    create_stage = SnowflakeOperator(task_id='create_stage', snowflake_conn_id='snowflake_v2', sql=f"""
        USE ROLE dbt_role;
        CREATE OR REPLACE STAGE sec_json_stage URL='{S3_URI}' CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}',AWS_SECRET_KEY='{AWS_SECRET_KEY}');
    """
    )
    create_fmt = SnowflakeOperator(task_id='create_fileformat_json', snowflake_conn_id='snowflake_v2', sql="CREATE OR REPLACE FILE FORMAT sec_json_format TYPE='JSON';")
    def schema_def_json(**kwargs):
        year=kwargs['dag_run'].conf.get('year'); q=kwargs['dag_run'].conf.get('quarter').lstrip('Q')
        return f"CREATE OR REPLACE TABLE SEC_JSON_{year}_Q{q}(json_data VARIANT);"
    gen_ddl = PythonOperator(task_id='generate_ddl_json', python_callable=schema_def_json, provide_context=True)
    ddl = SnowflakeOperator(task_id='ddl_json', snowflake_conn_id='snowflake_v2', sql="{{ ti.xcom_pull(task_ids='generate_ddl_json') }}")
    def copyinto_json(**kwargs):
        y=kwargs['dag_run'].conf.get('year'); q=kwargs['dag_run'].conf.get('quarter').lstrip('Q')
        return f"""
            COPY INTO SEC_JSON_{y}_Q{q} FROM @sec_json_stage/data/{y}/{q}/json/ FILE_FORMAT=(TYPE='JSON');
        """
    gen_copy = PythonOperator(task_id='generate_copy_sql_json', python_callable=copyinto_json, provide_context=True)
    copy_json = SnowflakeOperator(task_id='copy_from_s3_json', snowflake_conn_id='snowflake_v2', sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_json') }}")

    # Set dependencies explicitly
    start >> check >> branch >> [load, skip] >> join >> length >> compute >> pg >> create_stage >> create_fmt >> gen_ddl >> ddl >> gen_copy >> copy_json >> end
