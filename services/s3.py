#!/usr/bin/env python3
"""
S3FileManager: Utility class for interacting with AWS S3.
Provides methods to list objects, upload files (with optional retry), and generate presigned URLs.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Union

import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS credentials and bucket configuration
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3FileManager:
    """
    Manages file operations in an S3 bucket: listing, uploading, and presigned URL generation.

    Attributes:
        bucket_name (str): Name of the S3 bucket.
        base_path (str): Optional base prefix for all operations.
    """

    def __init__(
        self,
        bucket_name: str = AWS_BUCKET_NAME,
        base_path: str = ""
    ):
        """
        Initialize S3FileManager with AWS credentials loaded from environment.

        Args:
            bucket_name (str): Target S3 bucket name.
            base_path (str): Prefix for all S3 keys (no leading/trailing slash).
        """
        self.bucket_name = bucket_name
        self.base_path = base_path.strip("/")

        # Create S3 client using provided credentials
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List all object keys under the given prefix in the S3 bucket.

        Args:
            prefix (str): Subfolder or key prefix to filter objects.

        Returns:
            List[str]: List of object keys.
        """
        # Build full prefix path
        full_prefix = "/".join(filter(None, [self.base_path, prefix]))
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=full_prefix
            )
            contents = response.get('Contents', [])
            return [obj['Key'] for obj in contents]
        except Exception as e:
            logger.error(f"Error listing files at prefix '{full_prefix}': {e}")
            return []

    def upload_file(self, bucket_name: str, file_name: str, content: Union[str, bytes]) -> None:
        """
        Upload content to the specified bucket and key.

        Args:
            bucket_name (str): Destination bucket name.
            file_name (str): S3 key under which to store the object.
            content (str | bytes): Data to upload (raw bytes or string).
        """
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=content
            )
            logger.info(f"Uploaded object '{file_name}' to bucket '{bucket_name}'.")
        except Exception as e:
            logger.error(f"Failed to upload '{file_name}' to '{bucket_name}': {e}")
            raise

    def get_presigned_url(
        self,
        object_name: str,
        expiration: int = 3600
    ) -> Optional[str]:
        """
        Generate a presigned URL to download an S3 object.

        Args:
            object_name (str): Key of the object in S3.
            expiration (int): Time in seconds before the URL expires.

        Returns:
            str | None: Presigned URL or None if error.
        """
        # Build full key path
        full_key = "/".join(filter(None, [self.base_path, object_name]))
        try:
            url = self.s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={'Bucket': self.bucket_name, 'Key': full_key},
                ExpiresIn=expiration
            )
            logger.info(f"Generated presigned URL for '{full_key}'.")
            return url
        except Exception as e:
            logger.error(f"Error generating presigned URL for '{full_key}': {e}")
            return None

    def upload_with_retry(
        self,
        file_path: str,
        bucket_name: str,
        object_name: Optional[str] = None,
        max_attempts: int = 3
    ) -> bool:
        """
        Upload a local file to S3 with automatic retry on failure.

        Args:
            file_path (str): Local file system path to upload.
            bucket_name (str): Destination S3 bucket.
            object_name (str | None): S3 key for the uploaded file. Defaults to file_path.
            max_attempts (int): Number of retry attempts on failure.

        Returns:
            bool: True if upload succeeds, False otherwise.
        """
        # Configure multipart upload threshold and concurrency
        config = TransferConfig(
            multipart_threshold=25 * 1024 * 1024,  # 25 MB
            multipart_chunksize=25 * 1024 * 1024,
            max_concurrency=10,
            use_threads=True
        )

        target_key = object_name or file_path
        for attempt in range(1, max_attempts + 1):
            try:
                self.s3_client.upload_file(
                    Filename=file_path,
                    Bucket=bucket_name,
                    Key=target_key,
                    Config=config
                )
                logger.info(f"Upload succeeded on attempt {attempt} for '{target_key}'.")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed for '{target_key}': {e}")
                if attempt == max_attempts:
                    logger.error(f"All {max_attempts} upload attempts failed for '{target_key}'.")
        return False
