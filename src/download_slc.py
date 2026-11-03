#!/usr/bin/env python3
"""
Download Sentinel-1 SLC products (SAFE folders) from CDSE.

Searches the CDSE STAC catalogue for SLC products that match the given
orbit, date range and area, then downloads every product from the CDSE
S3 storage into `<base_path>/S1_<relative orbit>_<ASC|DES>/`.
Works for all Sentinel-1 satellites (S1A, S1B, S1C, S1D).

Parameters come from a YAML config (`--config`), from command-line flags,
or both; flags override the config. Run with `--help` for all options.

Exit code: 0 if every product is complete, 1 if any product failed,
2 for invalid parameters.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import requests

from cdse import (MissingCredentialsError, create_http_session,
                  create_s3_client, load_credentials)
from config import (ConfigError, build_slc_settings, flatten_config,
                    load_yaml_config, merge_cli_overrides)
from safe_download import (STATUS_DOWNLOADED, STATUS_FAILED, STATUS_SKIPPED,
                           download_product, log_transfer_speed)
from stac_search import search_scenes

logger = logging.getLogger("download_slc")

# Extra S3 connections on top of the transfer threads, e.g. for listing files.
EXTRA_S3_CONNECTIONS = 4


def parse_arguments(argv=None):
    """
    Define and parse the command-line options.

    Every option defaults to None so that it is possible to tell whether
    it was given (and should override the config) or not.

    Parameters
    ----------
    argv : list of str or None
        Arguments to parse; None means `sys.argv[1:]`.

    Returns
    -------
    argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Download Sentinel-1 SLC products from CDSE. "
                    "Command-line values override values from --config."
    )
    parser.add_argument("--config", help="YAML config file (optional)")

    search = parser.add_argument_group("search")
    search.add_argument("--orbit_state", help="ascending or descending")
    search.add_argument("--relative_orbit", type=int, help="relative orbit number (1-175)")
    search.add_argument("--instrument_mode", help="IW, EW, SM or WV (usually IW)")
    search.add_argument("--polarization", help="required polarization, default VV")
    search.add_argument("--start_date", help="first day, e.g. 2021-01-01")
    search.add_argument("--end_date", help="last day (inclusive), e.g. 2021-01-31")
    search.add_argument("--bbox", help='area as "WEST SOUTH EAST NORTH" in degrees')

    download = parser.add_argument_group("download")
    download.add_argument("--base_path", help="root folder for the downloaded data")
    download.add_argument("--max_threads", type=int, help="files downloaded in parallel (default 4)")
    download.add_argument("--threads_per_file", type=int,
                          help="parallel connections per large file (default 4)")
    download.add_argument("--max_retries", type=int, help="download attempts per product (default 3)")
    download.add_argument("--dry_run", action="store_true",
                          help="only search and list the products, do not download")

    return parser.parse_args(argv)


def build_run_folder_name(settings):
    """
    Name of the folder that groups all products of one track.

    Example: relative orbit 73, ascending -> "S1_073_ASC".
    """
    return f"S1_{settings.relative_orbit:03d}_{settings.orbit_state[:3].upper()}"


def log_settings(settings, output_dir):
    """Print the effective parameters so every run documents what it did."""
    logger.info("=== Effective parameters ===")
    logger.info("orbit_state      : %s", settings.orbit_state)
    logger.info("relative_orbit   : %s", settings.relative_orbit)
    logger.info("instrument_mode  : %s", settings.instrument_mode)
    logger.info("polarization     : %s", settings.polarization)
    logger.info("start_date       : %s", settings.start_date)
    logger.info("end_date         : %s (inclusive)", settings.end_date)
    logger.info("bbox             : %s", list(settings.bbox))
    logger.info("output folder    : %s", output_dir)
    logger.info("max_threads      : %s", settings.max_threads)
    logger.info("threads_per_file : %s", settings.threads_per_file)
    logger.info("max_retries      : %s", settings.max_retries)
    logger.info("============================")


def log_summary(results):
    """Print one line per failed product and the totals."""
    counts = {status: 0 for status in (STATUS_DOWNLOADED, STATUS_SKIPPED, STATUS_FAILED)}
    for result in results:
        counts[result.status] += 1

    logger.info("=== Summary ===")
    logger.info("Downloaded       : %d", counts[STATUS_DOWNLOADED])
    logger.info("Already complete : %d", counts[STATUS_SKIPPED])
    logger.info("Failed           : %d", counts[STATUS_FAILED])
    for result in results:
        if result.status == STATUS_FAILED:
            logger.error("  %s: %s", result.product_name, result.message)
    logger.info("===============")


def run(args):
    """
    Search and download all matching products.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Process exit code (0 = all products complete, 1 = some failed).

    Raises
    ------
    ConfigError
        If the parameters are missing or invalid.
    MissingCredentialsError
        If the CDSE credentials are missing.
    requests.RequestException
        If the STAC search fails after all retries.
    """
    cli_values = {key: value for key, value in vars(args).items() if key not in ("config", "dry_run")}
    values = merge_cli_overrides(flatten_config(load_yaml_config(args.config)), cli_values)
    settings = build_slc_settings(values)

    output_dir = Path(settings.base_path) / build_run_folder_name(settings)
    log_settings(settings, output_dir)

    scenes = search_scenes(create_http_session(), settings)
    logger.info("Found %d products", len(scenes))

    if args.dry_run:
        for scene in scenes:
            logger.info("  %s", scene.product_name)
        return 0

    access_key, secret_key = load_credentials()
    s3_client = create_s3_client(
        access_key, secret_key,
        max_connections=settings.max_threads * settings.threads_per_file + EXTRA_S3_CONNECTIONS,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for index, scene in enumerate(scenes, start=1):
        logger.info("[%d/%d] %s", index, len(scenes), scene.product_name)
        started_at = time.time()

        result = download_product(
            s3_client, scene, output_dir / scene.product_name,
            settings.max_threads, settings.threads_per_file, settings.max_retries,
        )
        results.append(result)

        if result.status == STATUS_DOWNLOADED:
            log_transfer_speed(result.downloaded_bytes, started_at)
        logger.info("  -> %s %s", result.status.upper(), result.message)

    log_summary(results)
    return 1 if any(r.status == STATUS_FAILED for r in results) else 0


def main():
    """Command-line entry point."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    # boto3 is chatty at INFO level about credentials and retries.
    logging.getLogger("botocore").setLevel(logging.WARNING)

    try:
        sys.exit(run(parse_arguments()))
    except ConfigError as error:
        logger.error("Invalid parameters: %s", error)
        sys.exit(2)
    except MissingCredentialsError as error:
        logger.error("%s", error)
        sys.exit(2)
    except requests.RequestException as error:
        logger.error("CDSE catalogue request failed: %s", error)
        sys.exit(1)


if __name__ == "__main__":
    main()
