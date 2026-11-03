"""
Download one Sentinel-1 SAFE product from CDSE S3 and verify it.

How a product is checked and downloaded:

1. List all files of the SAFE folder on S3 (with their sizes).
2. A local file counts as complete when it exists with the same size.
   If every file is complete, the product is skipped.
3. `manifest.safe` is downloaded first. It contains an MD5 checksum for
   most files of the product (annotation, measurement, ...).
4. All missing or incomplete files are downloaded in parallel. Each file
   that was just downloaded is checked against its MD5 from the manifest;
   a file with a wrong checksum is deleted so that the next attempt
   downloads it again.
5. Steps 2-4 are repeated up to `max_retries` times.

Already complete files from earlier runs are only checked by size, so
re-running over a large archive stays fast.
"""

import concurrent.futures
import hashlib
import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

from boto3.s3.transfer import TransferConfig

logger = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.safe"
CHECKSUM_READ_CHUNK = 8 * 1024 * 1024  # bytes read at a time when hashing
BYTES_PER_GB = 1024**3

STATUS_SKIPPED = "skipped"
STATUS_DOWNLOADED = "downloaded"
STATUS_FAILED = "failed"


class ChecksumError(RuntimeError):
    """Raised when a downloaded file does not match the MD5 in the manifest."""


@dataclass(frozen=True)
class RemoteFile:
    """One file of a SAFE product on S3."""

    key: str  # full S3 key
    relative_path: str  # path inside the SAFE folder, e.g. "measurement/x.tiff"
    size: int  # bytes


@dataclass
class ProductResult:
    """Outcome of downloading one product."""

    product_name: str
    status: str  # STATUS_SKIPPED, STATUS_DOWNLOADED or STATUS_FAILED
    downloaded_bytes: int = 0
    message: str = ""


def list_product_files(s3_client, bucket, prefix):
    """
    List all files of a SAFE product on S3.

    Zero-byte "directory marker" objects (keys ending in "/") are ignored.

    Parameters
    ----------
    s3_client : botocore.client.S3
    bucket : str
    prefix : str
        Key prefix of the SAFE folder, ending in "/".

    Returns
    -------
    list of RemoteFile
    """
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            files.append(
                RemoteFile(
                    key=obj["Key"],
                    relative_path=obj["Key"][len(prefix):],
                    size=obj["Size"],
                )
            )

    return files


def local_path_for(target_dir, remote_file):
    """
    Return where a remote file is stored locally.

    Raises
    ------
    ValueError
        If the S3 key would place the file outside `target_dir`
        (protects against unexpected keys such as "../x").
    """
    target_dir = Path(target_dir).resolve()
    path = (target_dir / remote_file.relative_path).resolve()

    if not path.is_relative_to(target_dir):
        raise ValueError(f"Refusing to write outside the product folder: {remote_file.key}")

    return path


def is_complete(target_dir, remote_file):
    """Return True if the local copy exists and has the remote size."""
    path = local_path_for(target_dir, remote_file)
    return path.is_file() and path.stat().st_size == remote_file.size


def find_incomplete_files(target_dir, remote_files):
    """Return the remote files whose local copy is missing or has the wrong size."""
    return [f for f in remote_files if not is_complete(target_dir, f)]


def read_manifest_checksums(manifest_path):
    """
    Read the MD5 checksums listed in a `manifest.safe` file.

    Parameters
    ----------
    manifest_path : Path

    Returns
    -------
    dict
        Maps the path inside the SAFE folder (e.g. "measurement/x.tiff")
        to its lowercase MD5 hex string. Empty if the file is missing.

    Raises
    ------
    xml.etree.ElementTree.ParseError
        If the manifest is not valid XML.
    """
    if not Path(manifest_path).is_file():
        return {}

    checksums = {}
    root = ET.parse(manifest_path).getroot()

    # Tags use XML namespaces ("{urn:...}byteStream"); match on the local name
    # so the code does not depend on the exact namespace URI.
    for byte_stream in root.iter():
        if not byte_stream.tag.endswith("byteStream"):
            continue

        location = checksum = None
        for child in byte_stream:
            if child.tag.endswith("fileLocation"):
                location = child.get("href")
            elif child.tag.endswith("checksum") and child.get("checksumName", "").upper() == "MD5":
                checksum = (child.text or "").strip().lower()

        if location and checksum:
            checksums[location.removeprefix("./")] = checksum

    return checksums


def md5_of_file(path):
    """Compute the MD5 hex digest of a file, reading it in chunks."""
    digest = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(CHECKSUM_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(s3_client, bucket, remote_file, target_dir, transfer_config, expected_md5=None):
    """
    Download one file and, if an MD5 is known, verify it.

    boto3 writes into a temporary file and renames it at the end, so an
    interrupted download never leaves a file with the final name behind.

    Parameters
    ----------
    s3_client : botocore.client.S3
    bucket : str
    remote_file : RemoteFile
    target_dir : Path
        Local SAFE folder.
    transfer_config : boto3.s3.transfer.TransferConfig
    expected_md5 : str or None
        Checksum from the manifest, if the manifest lists this file.

    Raises
    ------
    ChecksumError
        If the size or checksum does not match. The bad file is deleted.
    botocore.exceptions.BotoCoreError, ClientError
        If the download itself fails after boto3's own retries.
    """
    path = local_path_for(target_dir, remote_file)
    path.parent.mkdir(parents=True, exist_ok=True)

    s3_client.download_file(bucket, remote_file.key, str(path), Config=transfer_config)

    actual_size = path.stat().st_size
    if actual_size != remote_file.size:
        path.unlink()
        raise ChecksumError(f"size {actual_size} B, expected {remote_file.size} B")

    if expected_md5 and md5_of_file(path) != expected_md5:
        path.unlink()
        raise ChecksumError("MD5 does not match manifest")


def download_files_parallel(s3_client, bucket, remote_files, target_dir, checksums,
                            max_threads, threads_per_file):
    """
    Download several files of one product at the same time.

    Errors are collected instead of raised, so one bad file does not stop
    the other downloads.

    Parameters
    ----------
    s3_client : botocore.client.S3
    bucket : str
    remote_files : list of RemoteFile
    target_dir : Path
    checksums : dict
        MD5 per relative path (see `read_manifest_checksums`).
    max_threads : int
        Number of files downloaded at the same time.
    threads_per_file : int
        Number of parallel connections used for each large file.

    Returns
    -------
    tuple of (int, list of str)
        Bytes downloaded successfully, and one error message per failed file.
    """
    transfer_config = TransferConfig(max_concurrency=threads_per_file)
    downloaded_bytes = 0
    errors = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(
                download_file, s3_client, bucket, remote_file, target_dir,
                transfer_config, checksums.get(remote_file.relative_path),
            ): remote_file
            for remote_file in remote_files
        }

        for done_count, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            remote_file = futures[future]
            try:
                future.result()
            except Exception as error:  # collect any failure; it is retried later
                errors.append(f"{remote_file.relative_path}: {error}")
                logger.warning("  FAILED %s: %s", remote_file.relative_path, error)
                continue

            downloaded_bytes += remote_file.size
            logger.info(
                "  [%d/%d] %s (%.2f GB)", done_count, len(remote_files),
                remote_file.relative_path, remote_file.size / BYTES_PER_GB,
            )

    return downloaded_bytes, errors


def download_product(s3_client, scene, target_dir, max_threads, threads_per_file, max_retries):
    """
    Make sure one SAFE product is completely and correctly available locally.

    Parameters
    ----------
    s3_client : botocore.client.S3
    scene : stac_search.Scene
    target_dir : Path
        Local SAFE folder (`.../<product>.SAFE`).
    max_threads, threads_per_file : int
        See `download_files_parallel`.
    max_retries : int
        How many download rounds are attempted before giving up.

    Returns
    -------
    ProductResult
        Never raises for download problems; they are reported in the result.
    """
    remote_files = list_product_files(s3_client, scene.bucket, scene.prefix)
    if not remote_files:
        return ProductResult(scene.product_name, STATUS_FAILED,
                             message=f"No files found at s3://{scene.bucket}/{scene.prefix}")

    if not find_incomplete_files(target_dir, remote_files):
        return ProductResult(scene.product_name, STATUS_SKIPPED, message="already complete")

    total_gb = sum(f.size for f in remote_files) / BYTES_PER_GB
    logger.info("  %d files, %.2f GB in total", len(remote_files), total_gb)

    downloaded_bytes = 0
    errors = []

    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info("  Retry %d/%d", attempt, max_retries)

        attempt_bytes, errors = _download_round(
            s3_client, scene.bucket, remote_files, target_dir, max_threads, threads_per_file
        )
        downloaded_bytes += attempt_bytes

        if not errors and not find_incomplete_files(target_dir, remote_files):
            return ProductResult(scene.product_name, STATUS_DOWNLOADED, downloaded_bytes)

    return ProductResult(
        scene.product_name, STATUS_FAILED, downloaded_bytes,
        message=f"{len(errors)} file(s) still failing after {max_retries} attempts: "
                + "; ".join(errors[:3]),
    )


def _download_round(s3_client, bucket, remote_files, target_dir, max_threads, threads_per_file):
    """
    Run one download round: manifest first, then all other incomplete files.

    Returns
    -------
    tuple of (int, list of str)
        Bytes downloaded and error messages, as in `download_files_parallel`.
    """
    incomplete = find_incomplete_files(target_dir, remote_files)
    manifest = [f for f in incomplete if f.relative_path == MANIFEST_NAME]
    others = [f for f in incomplete if f.relative_path != MANIFEST_NAME]

    # The manifest must be on disk before the other files, because it
    # holds the checksums used to verify them.
    downloaded_bytes, errors = download_files_parallel(
        s3_client, bucket, manifest, target_dir, {}, 1, threads_per_file
    )
    if errors:
        return downloaded_bytes, errors

    try:
        checksums = read_manifest_checksums(Path(target_dir) / MANIFEST_NAME)
    except ET.ParseError as error:
        (Path(target_dir) / MANIFEST_NAME).unlink(missing_ok=True)
        return downloaded_bytes, [f"{MANIFEST_NAME} is not valid XML ({error}), deleted"]

    other_bytes, errors = download_files_parallel(
        s3_client, bucket, others, target_dir, checksums, max_threads, threads_per_file
    )
    return downloaded_bytes + other_bytes, errors


def log_transfer_speed(downloaded_bytes, started_at):
    """Log how much was downloaded and the average speed since `started_at`."""
    duration = max(time.time() - started_at, 1e-6)
    gigabytes = downloaded_bytes / BYTES_PER_GB
    megabytes_per_second = downloaded_bytes / (1024**2) / duration
    logger.info(
        "  Downloaded %.2f GB in %.0f s (%.1f MB/s, %.0f Mbit/s)",
        gigabytes, duration, megabytes_per_second, megabytes_per_second * 8,
    )
