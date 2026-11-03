"""
Shared access to the Copernicus Data Space Ecosystem (CDSE).

This module holds everything both downloaders need to talk to CDSE:
the service endpoints, reading the S3 credentials, an HTTP session that
retries temporary failures, and a configured S3 client.
"""

import os
from pathlib import Path

import boto3
import requests
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

STAC_SEARCH_URL = "https://stac.dataspace.copernicus.eu/v1/search"
ODATA_PRODUCTS_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
S3_ENDPOINT_URL = "https://eodata.dataspace.copernicus.eu"

ACCESS_KEY_ENV = "cdse_S3_KEY"
SECRET_KEY_ENV = "cdse_S3_SECRET"

# Seconds to wait for a connection / for the server to answer.
# Without a timeout, a stalled connection would hang the script forever.
HTTP_TIMEOUT = (30, 120)

# CDSE sometimes answers with "too many requests" or temporary server
# errors. These are worth retrying; anything else is a real error.
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
HTTP_MAX_RETRIES = 5
HTTP_BACKOFF_SECONDS = 2

S3_MAX_RETRIES = 10

# CDSE quota for "Copernicus General Users" (documentation.dataspace.copernicus.eu/Quotas.html):
# at most 4 concurrent S3 connections, 20 MB/s per connection, 2000 requests
# per minute. More connections are rejected with HTTP 429 "Too Many Requests".
# The connection limit is enforced in config.py (max_connections).

# botocore's "standard"/"adaptive" retry modes only recognise throttling by
# AWS error names (e.g. "SlowDown"). CDSE answers with a bare HTTP 429, which
# only the "legacy" mode retries (with exponential backoff).
S3_RETRY_MODE = "legacy"

REPO_DIR = Path(__file__).resolve().parent.parent


class MissingCredentialsError(RuntimeError):
    """Raised when the CDSE S3 credentials are not set."""


def load_credentials():
    """
    Read the CDSE S3 credentials from the environment.

    A `.env` file in the repository root is loaded first if it exists.
    Variables that are already set in the environment take precedence.

    Returns
    -------
    tuple of (str, str)
        The S3 access key and secret key.

    Raises
    ------
    MissingCredentialsError
        If one of the two variables is missing.
    """
    load_dotenv(REPO_DIR / ".env")

    access_key = os.getenv(ACCESS_KEY_ENV)
    secret_key = os.getenv(SECRET_KEY_ENV)

    if not access_key or not secret_key:
        raise MissingCredentialsError(
            f"Missing CDSE credentials. Set {ACCESS_KEY_ENV} and "
            f"{SECRET_KEY_ENV} in the environment or in {REPO_DIR / '.env'}."
        )

    return access_key, secret_key


def create_http_session():
    """
    Create a requests session that retries temporary CDSE failures.

    POST is included in the retried methods because the STAC search is a
    read-only query, so repeating it is safe.

    Returns
    -------
    requests.Session
        Session with automatic retries and exponential backoff.
    """
    retry = Retry(
        total=HTTP_MAX_RETRIES,
        backoff_factor=HTTP_BACKOFF_SECONDS,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=["GET", "POST"],
        respect_retry_after_header=True,
    )

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def create_s3_client(access_key, secret_key, max_connections):
    """
    Create an S3 client for the CDSE `eodata` storage.

    boto3 clients are thread-safe, so one client can be shared by all
    download threads.

    Parameters
    ----------
    access_key, secret_key : str
        CDSE S3 credentials.
    max_connections : int
        Size of the connection pool. Set it to the number of parallel
        transfers, so no idle extra connections count against the CDSE limit.

    Returns
    -------
    botocore.client.S3
        Configured S3 client.
    """
    boto_config = BotoConfig(
        retries={"max_attempts": S3_MAX_RETRIES, "mode": S3_RETRY_MODE},
        max_pool_connections=max_connections,
        connect_timeout=HTTP_TIMEOUT[0],
        read_timeout=HTTP_TIMEOUT[1],
    )

    return boto3.session.Session().client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
        config=boto_config,
    )


def split_s3_url(s3_url):
    """
    Split an S3 location into bucket and key.

    Accepts both `s3://eodata/Sentinel-1/...` (STAC style) and
    `/eodata/Sentinel-1/...` (OData style).

    Parameters
    ----------
    s3_url : str
        S3 location.

    Returns
    -------
    tuple of (str, str)
        Bucket name and object key (or key prefix).

    Raises
    ------
    ValueError
        If the location does not contain both a bucket and a key.
    """
    path = s3_url.removeprefix("s3://").lstrip("/")
    bucket, _, key = path.partition("/")

    if not bucket or not key:
        raise ValueError(f"Not a valid S3 location: {s3_url}")

    return bucket, key
