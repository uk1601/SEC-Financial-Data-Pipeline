#!/usr/bin/env python3
"""
Parquet Conversion Module

Provides utilities to fetch SEC tickers and transform extracted SEC data files into optimized Parquet format.
"""
import logging
from io import BytesIO, StringIO
from typing import List, Tuple

import pandas as pd
import requests

# Configure module-level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_ticker_file() -> List[Tuple[str, BytesIO]]:
    """
    Fetches the SEC ticker file (ticker.txt) and converts it to Parquet format in memory.

    Returns:
        List of tuples containing the target filename and a BytesIO buffer of Parquet data.

    Raises:
        RuntimeError: If the HTTP request fails or transformation errors occur.
    """
    url = "https://www.sec.gov/include/ticker.txt"
    headers = {
        # SEC policy: include valid contact email
        "User-Agent": "your_email@example.com",
        "Accept": "text/plain"
    }

    logger.info(f"Downloading SEC ticker file from {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as err:
        logger.error(f"HTTP request failed: {err}")
        raise RuntimeError(f"Failed to fetch ticker file: {err}")

    try:
        # Read into DataFrame
        df = pd.read_table(
            StringIO(response.text),
            delimiter="\t",
            header=None,
            names=["symbol", "cik"],
            dtype={"symbol": str, "cik": str}
        )
        # Convert to Parquet in-memory
        buffer = BytesIO()
        df.to_parquet(
            buffer,
            index=False,
            compression="snappy",
            engine="pyarrow"
        )
        buffer.seek(0)
        logger.info("Ticker file transformed to Parquet format in memory.")
        return [("ticker.parquet", buffer)]
    except Exception as err:
        logger.error(f"Transformation error: {err}")
        raise RuntimeError(f"Failed to convert ticker to Parquet: {err}")


def parquet_transformer(
    extracted_files: List[Tuple[str, BytesIO]],
    year: int,
    quarter: int
) -> List[Tuple[str, BytesIO]]:
    """
    Transforms a list of extracted text files into Parquet format.

    Each TXT file in extracted_files is parsed into a DataFrame, enriched with
    year and quarter columns, cast certain columns to categorical, and written
    to a BytesIO Parquet buffer.

    Args:
        extracted_files: List of (filename, BytesIO) tuples for TXT files.
        year: Four-digit year for ingestion metadata.
        quarter: Quarter number (1-4) for ingestion metadata.

    Returns:
        List of (new_parquet_filename, BytesIO) tuples for each transformed file.
    """
    transformed: List[Tuple[str, BytesIO]] = []
    categorical_cols = ["tag", "version", "uom", "segments", "form", "stmt"]

    for filename, buf in extracted_files:
        # Only transform .txt files
        if not filename.lower().endswith('.txt'):
            logger.debug(f"Skipping non-TXT file: {filename}")
            continue

        parquet_name = f"{filename.rsplit('.', 1)[0]}.parquet"
        try:
            buf.seek(0)
            df = pd.read_table(buf, delimiter="\t", low_memory=False)
            # Add ingestion metadata
            df['year'] = year
            df['quarter'] = quarter
            # Convert specified columns to categorical if present
            for col in categorical_cols:
                if col in df.columns:
                    df[col] = df[col].astype('category')

            out_buffer = BytesIO()
            df.to_parquet(
                out_buffer,
                index=False,
                compression='snappy',
                engine='pyarrow'
            )
            out_buffer.seek(0)
            transformed.append((parquet_name, out_buffer))
            logger.info(f"Transformed '{filename}' to '{parquet_name}'.")
        except Exception as err:
            logger.error(f"Failed to transform {filename}: {err}")
            # Continue processing other files

    return transformed
