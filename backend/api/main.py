#!/usr/bin/env python3
"""
US SEC Data Bridge API
FastAPI application for checking Snowflake data availability and executing read-only queries.
"""
import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import registry

# Load environment variables from .env file
load_dotenv()

# Configure logging for the module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Register the Snowflake dialect with SQLAlchemy
registry.register("snowflake", "snowflake.sqlalchemy", "dialect")


def get_snowflake_engine(schema: str = None):
    """
    Create and return a SQLAlchemy engine for Snowflake.

    Args:
        schema (str, optional): The Snowflake schema to connect to. Defaults to SNOWFLAKE_SCHEMA env var.

    Returns:
        sqlalchemy.Engine: Configured engine for Snowflake.
    """
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSCODE')
    account = os.getenv('SNOWFLAKE_ACCOUNT')  # e.g., xyz123.us-east-1
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = schema or os.getenv('SNOWFLAKE_SCHEMA')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    role = os.getenv('SNOWFLAKE_ROLE')

    connection_url = (
        f"snowflake://{user}:{password}@{account}/{database}/{schema}"
        f"?warehouse={warehouse}&role={role}"
    )
    return create_engine(connection_url)


# Initialize FastAPI application
app = FastAPI(
    title="US SEC Data Bridge API",
    description="APIs to check record counts and execute safe read-only queries against Snowflake.",
    version="1.0"
)


@app.get("/check-availability")
async def check_data_availability(
    query: str = Query(
        ...,
        description="SQL query (COUNT) to verify data availability in INFORMATION_SCHEMA."
    )
):
    """
    Execute a record-count query in Snowflake INFORMATION_SCHEMA to check data availability.

    Returns:
        JSON with 'data': list of rows (dicts) returned by the query.
    """
    try:
        # Connect to INFORMATION_SCHEMA for metadata queries
        engine = get_snowflake_engine(schema="INFORMATION_SCHEMA")
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(row) for row in result]
        return {"data": rows}

    except Exception as e:
        logger.error(f"Error checking data availability: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error executing availability check: {e}"
        )


@app.get("/query-data")
async def query_data(
    query: str = Query(
        ...,
        description="Read-only SQL query (SELECT, SHOW, DESC) to execute against Snowflake."
    )
):
    """
    Execute safe read-only SQL queries on Snowflake (only SELECT, SHOW, DESCRIBE).

    Returns:
        JSON with 'data': list of rows (dicts) returned by the query.
    """
    sql = query.strip()
    opcode = sql.split()[0].lower()
    if opcode not in ("select", "show", "desc", "describe"):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT, SHOW, or DESCRIBE queries are allowed."
        )

    try:
        engine = get_snowflake_engine()
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = [dict(row) for row in result]
        return {"data": rows}

    except Exception as e:
        logger.error(f"Error executing query-data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error executing query: {e}"
        )
