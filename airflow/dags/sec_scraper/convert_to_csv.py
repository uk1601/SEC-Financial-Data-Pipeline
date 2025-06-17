#!/usr/bin/env python3
"""
CSV Conversion Module

Provides a transformer to convert extracted SEC data byte-streams into CSV format.
Each input .txt file is parsed into a DataFrame, enriched with year/quarter metadata,
and written to an in-memory CSV BytesIO buffer.
"""
import logging
from io import BytesIO
from typing import List, Tuple

import pandas as pd

# Configure module-level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def csv_transformer(
    extracted_files: List[Tuple[str, BytesIO]],
    year: int,
    quarter: int
) -> List[Tuple[str, BytesIO]]:
    """
    Transforms a list of extracted text files into CSV format.

    Args:
        extracted_files: List of (filename, BytesIO) tuples for TXT files.
        year: The fiscal year associated with the data.
        quarter: The quarter number associated with the data (1-4).

    Returns:
        List of (csv_filename, BytesIO) tuples for each transformed file.
    """
    transformed: List[Tuple[str, BytesIO]] = []

    for filename, buffer in extracted_files:
        # Only process .txt files
        if not filename.lower().endswith('.txt'):
            logger.debug(f"Skipping non-TXT file: {filename}")
            continue

        csv_name = f"{filename.rsplit('.', 1)[0]}.csv"
        try:
            # Reset buffer pointer and read into DataFrame
            buffer.seek(0)
            df = pd.read_table(buffer, delimiter="\t", low_memory=False)

            # Enrich with ingestion metadata
            df['year'] = year
            df['quarter'] = quarter

            # Write DataFrame to CSV in-memory
            out_buffer = BytesIO()
            df.to_csv(out_buffer, index=False)
            out_buffer.seek(0)

            transformed.append((csv_name, out_buffer))
            logger.info(f"Transformed '{filename}' to '{csv_name}'.")

        except Exception as err:
            logger.error(f"Failed to transform '{filename}' to CSV: {err}")
            # Continue processing remaining files

    return transformed
