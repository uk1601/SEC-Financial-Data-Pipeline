#!/usr/bin/env python3
"""
US Securities and Exchange Commission Financial Data Pipeline
Streamlit application to trigger Airflow DAGs and query Snowflake data.
"""
import os
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit_option_menu import option_menu

# Load environment variables
load_dotenv()

# Constants for API endpoints (set via .env file)
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL")
QUERY_API_URL = os.getenv("QUERY_API_URL")


def populate_schema(source: str, year: int, quarter: str) -> None:
    """
    Display the column schema for a selected SEC table based on the data source.

    Args:
        source (str): One of "FACT", "RAW", or other ("JSON").
        year (int): Fiscal year.
        quarter (str): Fiscal quarter (e.g., 'Q1').
    """
    # Define column lists for each table category
    if source == "FACT":
        schemas = {
            "Balance_Sheet": [
                "COMPANY_NAME", "COMPANY_ID", "FILING_DATE", "PERIOD",
                "FISCAL_YEAR", "FISCAL_PERIOD", "UNIT", "PREFERRED_LABEL",
                "TOTAL_REPORTED_AMOUNT", "TAG", "DATATYPE", "DOCUMENTATION"
            ],
            "Cash_Flow": [
                "COMPANY_NAME", "COMPANY_ID", "FILING_DATE", "PERIOD",
                "FISCAL_YEAR", "FISCAL_PERIOD", "UNIT", "PREFERRED_LABEL",
                "TOTAL_REPORTED_AMOUNT", "TAG", "DATATYPE", "DOCUMENTATION"
            ],
            "Income_Statement": [
                "COMPANY_NAME", "COMPANY_ID", "FILING_DATE", "PERIOD",
                "FISCAL_YEAR", "FISCAL_PERIOD", "UNIT", "PREFERRED_LABEL",
                "TOTAL_REPORTED_AMOUNT", "TAG", "DATATYPE", "DOCUMENTATION"
            ]
        }
    elif source == "RAW":
        # Template names include year and quarter
        schemas = {
            f"STG_NUM_{year}_{quarter}": [
                "SUBMISSION_ID", "TAG", "VERSION", "PERIOD_END_DATE",
                "NUM_QUATERS_COVERED", "UNIT", "SEGMENTS", "COREG",
                "REPORTED_AMOUNT", "FOOTNOTE"
            ],
            f"STG_PRE_{year}_{quarter}": [
                "SUBMISSION_ID", "REPORT", "LINE", "STATEMENT_TYPE",
                "DIRECTLY_REPORTED", "RFILE", "TAG", "VERSION",
                "PREFERRED_LABEL", "NEGATING"
            ],
            f"STG_SUB_{year}_{quarter}": [
                "SUBMISSION_ID", "COMPANY_ID", "COMPANY_NAME", "SIC_CODE",
                "BUSINESS_COUNTRY", "BUSINESS_STATE", "BUSINESS_CITY",
                "BUSINESS_ZIP", "BUSINESS_ADD_1", "BUSINESS_ADD_2",
                "BUSINESS_PH_NO", "MAILING_COUNTRY", "MAILING_STATE",
                "MAILING_CITY", "MAILING_ZIP", "REGISTRANT_MAIL_ADD_1",
                "REGISTRANT_MAIL_ADD_2", "COUNTRYINC", "STATE_PROV_INC",
                "EMPLOYER_ID", "FORMER", "CHANGED", "AFS", "WKSI", "FYE",
                "FORM", "PERIOD", "FILING_DATE", "FISCAL_YEAR", "FISCAL_PERIOD",
                "ACCEPTED", "PREVRPT", "DETAIL", "INSTANCE", "NCIKS", "ACIKS"
            ],
            f"STG_TAG_{year}_{quarter}": [
                "TAG", "VERSION", "CUSTOM", "ABSTRACT", "DATATYPE",
                "ITEM_ORDER", "BALANCE_TYPE", "TAG_LABEL", "DOCUMENTATION"
            ]
        }
    else:
        # Default to JSON data staging
        schemas = {
            f"SEC_JSON_{year}_{quarter}": ["JSON_DATA"]
        }

    # Let user select a table and display its schema
    table_list = list(schemas.keys())
    selected = st.selectbox("Choose Table:", options=table_list)

    # Create DataFrame for display
    df = pd.DataFrame({selected: schemas[selected]})
    st.write(f"**{selected} Schema:**")
    st.dataframe(df, use_container_width=True, hide_index=True)


def populate_airflow_page() -> None:
    """
    Streamlit page for triggering Airflow DAGs via REST API.
    """
    st.subheader("Trigger Airflow DAGs")

    # Input: source, year, quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT"]).lower()
    with col2:
        year = st.selectbox("Select Year", range(2024, 2009, -1))
    with col3:
        quarter = st.selectbox("Select Quarter", ("Q1", "Q2", "Q3", "Q4"))

    # Button to trigger the DAG
    if st.button("Trigger Airflow DAG", use_container_width=True):
        payload = {"conf": {"source": source, "year": year, "quarter": quarter}}
        endpoint = f"{AIRFLOW_API_URL}/api/v1/dags/sec_{source}_data_to_snowflake/dagRuns"
        auth = (os.getenv("AIRFLOW_USER"), os.getenv("AIRFLOW_PASSCODE"))

        response = requests.post(endpoint, json=payload, auth=auth)
        if response.status_code == 200:
            st.success("DAG triggered successfully!")
        else:
            st.error(f"Failed to trigger DAG: {response.status_code} - {response.text}")


def generate_check_query(source: str, year: int, quarter: str) -> str:
    """
    Build an SQL query to verify table availability in Snowflake.

    Returns:
        str: SQL query string.
    """
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    if source.upper() == "RAW":
        tables = [
            f"STG_NUM_{year}_{quarter}",
            f"STG_PRE_{year}_{quarter}",
            f"STG_SUB_{year}_{quarter}",
            f"STG_TAG_{year}_{quarter}"
        ]
    elif source.upper() == "JSON":
        tables = [f"SEC_JSON_{year}_{quarter}"]
    else:
        tables = ["BALANCE_SHEET", "CASH_FLOW", "INCOME_STATEMENT"]

    table_list = ", ".join(f"'{t}'" for t in tables)
    return (
        f"SELECT COUNT(*) AS RECORD_COUNT "
        f"FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA='{schema}' "
        f"AND TABLE_NAME IN ({table_list});"
    )


def check_data_availability(query: str) -> list:
    """
    Query the backend to check record count for availability.

    Returns:
        list: JSON data from API response.
    """
    try:
        response = requests.get(f"{QUERY_API_URL}/check-availability", params={"query": query})
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        st.error(f"Error checking data availability: {e}")
        return []


def execute_query(query: str) -> list:
    """
    Execute a user-provided SQL query against Snowflake via backend.

    Returns:
        list: JSON data from API response.
    """
    try:
        response = requests.get(f"{QUERY_API_URL}/query-data", params={"query": query})
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return []


def populate_query_page() -> None:
    """
    Streamlit page for checking data availability and running SQL queries.
    """
    st.subheader("Query Snowflake")

    # Initialize session state
    if "flag" not in st.session_state:
        st.session_state.flag = False
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "prev_inputs" not in st.session_state:
        st.session_state.prev_inputs = {"source": None, "year": None, "quarter": None}

    # Input: source, year, quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT"])
    with col2:
        year = st.selectbox("Select Year", range(2024, 2009, -1))
    with col3:
        quarter = st.selectbox("Select Quarter", ("Q1", "Q2", "Q3", "Q4"))

    current = {"source": source, "year": year, "quarter": quarter}
    if current != st.session_state.prev_inputs:
        st.session_state.flag = False
        st.session_state.prev_inputs = current

    # Check availability button
    if st.button("Check Availability", use_container_width=True):
        availability_query = generate_check_query(source, year, quarter)
        result = check_data_availability(availability_query)
        if result and result[0].get("record_count", 0) > 0:
            st.success(f"Data available for {source}, Year {year}, Quarter {quarter}.")
            st.session_state.flag = True
        else:
            st.info(f"No data for {source}, Year {year}, Quarter {quarter}. Trigger Airflow DAG to fetch.")

    # If data exists, display schema and query interface
    if st.session_state.flag:
        populate_schema(source, year, quarter)

        st.session_state.query = st.text_area(
            "Enter your query here:",
            value=st.session_state.query,
            height=200,
            key="query_input"
        )
        if st.button("Run Query", use_container_width=True):
            if st.session_state.query.strip():
                df = execute_query(st.session_state.query)
                if df:
                    st.write("Query Results:")
                    st.dataframe(df)
                    st.success("Query executed successfully.")
                else:
                    st.warning("Query returned no results.")
            else:
                st.error("Please enter a valid SQL query.")


def main() -> None:
    """
    Main entry point for the Streamlit app.
    """
    st.set_page_config(
        page_title="US SEC - Data Bridge",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("US Securities and Exchange Commission")
    st.header("Financial Data Pipeline")

    # Sidebar menu for navigation
    with st.sidebar:
        choice = option_menu(
            menu_title="Main Menu",
            options=["Airflow", "Query Snowflake"],
            icons=["rocket", "database"],
            default_index=0
        )

    # Render selected page
    if choice == "Airflow":
        populate_airflow_page()
    else:
        populate_query_page()


if __name__ == "__main__":
    main()
