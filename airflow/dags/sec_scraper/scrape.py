#!/usr/bin/env python3
"""
SEC Scraper Module

Downloads and extracts SEC financial statement data ZIP archives into in-memory byte streams.
"""
import logging
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import List, Tuple

import requests
from zipfile import ZipFile

# Configure module-level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_zip_to_bytes(response: requests.Response) -> List[Tuple[str, BytesIO]]:
    """
    Extracts all files from a ZIP archive contained in an HTTP response.

    Args:
        response (requests.Response): HTTP response with ZIP content.

    Returns:
        List[Tuple[str, BytesIO]]: A list of tuples containing each file name and its byte stream.

    Raises:
        ValueError: If the ZIP archive contains no files.
    """
    # Write response content to a temporary file to handle large ZIPs efficiently
    with NamedTemporaryFile(prefix="sec_data_", suffix=".zip") as temp_file:
        temp_file.write(response.content)
        temp_file.flush()

        with ZipFile(temp_file.name, 'r') as archive:
            members = archive.namelist()
            if not members:
                raise ValueError("No files found in the ZIP archive.")

            extracted: List[Tuple[str, BytesIO]] = []
            for member in members:
                with archive.open(member) as file_obj:
                    data = file_obj.read()
                    extracted.append((member, BytesIO(data)))
                    logger.debug(f"Extracted '{member}' ({len(data)} bytes)")

            return extracted


def scrape_sec_data(year: int, quarter: int) -> List[Tuple[str, BytesIO]]:
    """
    Downloads and extracts SEC financial-statement ZIP for a given year and quarter.

    The SEC requires a valid User-Agent. Update headers['User-Agent'] with your contact.

    Args:
        year (int): Four-digit year (e.g., 2024).
        quarter (int): Quarter number (1 to 4).

    Returns:
        List[Tuple[str, BytesIO]]: Extracted files as (filename, byte-stream) tuples.

    Raises:
        RuntimeError: If download fails or ZIP processing fails.
    """
    if quarter not in {1, 2, 3, 4}:
        raise ValueError(f"Invalid quarter '{quarter}'. Must be 1, 2, 3, or 4.")

    # Construct download URL
    url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
    headers = {
        # SEC policy: include valid contact email
        "User-Agent": "your_email@example.com",
        "Accept": "application/zip,application/octet-stream",
        "Host": "www.sec.gov",
        "Referer": "https://www.sec.gov/"
    }

    logger.info(f"Downloading SEC data: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        logger.error(f"Download error: {err}")
        raise RuntimeError(f"Failed to download SEC data: {err}")

    try:
        files = extract_zip_to_bytes(response)
        logger.info(f"Extracted {len(files)} files from ZIP archive.")
        return files
    except Exception as err:
        logger.error(f"ZIP extraction error: {err}")
        raise RuntimeError(f"Failed to extract SEC data: {err}")


if __name__ == "__main__":
    # Example usage
    try:
        year = 2024
        quarter = 2
        extracted_files = scrape_sec_data(year, quarter)
        for filename, stream in extracted_files:
            logger.info(f"File available: {filename} ({stream.getbuffer().nbytes} bytes)")
    except Exception as err:
        logger.error(f"Execution failed: {err}")
