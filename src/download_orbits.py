#!/usr/bin/env python3
"""
Download Sentinel-1 orbit files from CDSE for already downloaded SLC products.

Scans `safe_dir` (recursively) for `.SAFE` folders and makes sure
`orbit_dir` contains an orbit file for each of them:

- A precise orbit (POEORB) is used whenever CDSE has one.
- If only a restituted orbit (RESORB) exists, that is downloaded instead.
  On a later run, once the precise orbit is published, the precise orbit
  is downloaded and the RESORB file it replaces is deleted.

Works for all Sentinel-1 satellites (S1A, S1B, S1C, S1D). Orbit files come
from the CDSE catalogue and S3 storage, using the same credentials as the
SLC download.

Exit code: 0 if every scene has an orbit file, 1 if any scene failed,
2 for invalid parameters.
"""

import argparse
import logging
import sys
from pathlib import Path

import requests
from botocore.exceptions import BotoCoreError, ClientError

from cdse import (MissingCredentialsError, create_http_session,
                  create_s3_client, load_credentials)
from config import (ConfigError, build_orbit_settings, flatten_config,
                    load_yaml_config, merge_cli_overrides)
from orbits import (PRECISE, RESTITUTED, best_local_orbit, download_orbit,
                    list_local_orbits, parse_scene_name, query_cdse_orbit)

logger = logging.getLogger("download_orbits")

# Orbit files are small and downloaded one after another, so one connection
# is enough and leaves the rest of the CDSE limit (4) to a parallel SLC download.
S3_CONNECTIONS = 1

STATUS_SKIPPED = "skipped"
STATUS_DOWNLOADED = "downloaded"
STATUS_FAILED = "failed"


def parse_arguments(argv=None):
    """
    Define and parse the command-line options.

    Parameters
    ----------
    argv : list of str or None
        Arguments to parse; None means `sys.argv[1:]`.

    Returns
    -------
    argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Download Sentinel-1 orbit files from CDSE for local SAFE products. "
                    "Command-line values override values from --config."
    )
    parser.add_argument("--config", help="YAML config file with an 'orbits:' section (optional)")
    parser.add_argument("--safe_dir", help="folder that contains the .SAFE products (searched recursively)")
    parser.add_argument("--orbit_dir", help="folder where the orbit files are stored")
    return parser.parse_args(argv)


def find_safe_products(safe_dir):
    """
    Find all SAFE product folders below `safe_dir`.

    Returns
    -------
    list of Path
        Sorted list of `.SAFE` directories.
    """
    return sorted(path for path in Path(safe_dir).rglob("*.SAFE") if path.is_dir())


def ensure_orbit_for_scene(session, s3_client, scene, orbit_dir, local_orbits):
    """
    Make sure `orbit_dir` has the best available orbit file for one scene.

    `local_orbits` is updated in place when files are added or removed, so
    later scenes of the same run see the current state of the folder.

    Parameters
    ----------
    session : requests.Session
    s3_client : botocore.client.S3
    scene : orbits.SceneTiming
    orbit_dir : Path
    local_orbits : list of orbits.OrbitFile

    Returns
    -------
    tuple of (str, str)
        Status (STATUS_SKIPPED / STATUS_DOWNLOADED / STATUS_FAILED) and a message.
    """
    local = best_local_orbit(scene, local_orbits)
    if local and local.orbit_type == PRECISE:
        return STATUS_SKIPPED, f"precise orbit present: {local.name}"

    precise = query_cdse_orbit(session, scene, PRECISE)
    if precise:
        orbit = fetch_orbit(s3_client, precise, orbit_dir, local_orbits)
        remove_replaced_restituted_orbits(scene, orbit_dir, local_orbits)
        return STATUS_DOWNLOADED, f"{PRECISE}: {orbit.name}"

    # From here on no precise orbit exists yet.
    if local:
        return STATUS_SKIPPED, f"no precise orbit yet, restituted orbit present: {local.name}"

    restituted = query_cdse_orbit(session, scene, RESTITUTED)
    if restituted:
        orbit = fetch_orbit(s3_client, restituted, orbit_dir, local_orbits)
        return STATUS_DOWNLOADED, f"{RESTITUTED} (no precise orbit yet): {orbit.name}"

    return STATUS_FAILED, "no orbit file found on CDSE"


def fetch_orbit(s3_client, catalogue_result, orbit_dir, local_orbits):
    """
    Download an orbit file found in the catalogue and register it locally.

    Parameters
    ----------
    catalogue_result : tuple of (orbits.OrbitFile, str)
        Result of `query_cdse_orbit`.

    Returns
    -------
    orbits.OrbitFile
    """
    orbit, s3_path = catalogue_result
    download_orbit(s3_client, s3_path, orbit_dir)
    local_orbits.append(orbit)
    return orbit


def remove_replaced_restituted_orbits(scene, orbit_dir, local_orbits):
    """
    Delete local RESORB files for a scene that now has a precise orbit.

    Keeping both would let processing tools pick the less accurate one.
    """
    for orbit in list(local_orbits):
        if orbit.orbit_type == RESTITUTED and orbit.covers(scene):
            (Path(orbit_dir) / orbit.name).unlink(missing_ok=True)
            local_orbits.remove(orbit)
            logger.info("    Removed replaced restituted orbit: %s", orbit.name)


def run(args):
    """
    Download orbit files for all SAFE products found in `safe_dir`.

    Parameters
    ----------
    args : argparse.Namespace

    Returns
    -------
    int
        Process exit code (0 = all scenes have an orbit, 1 = some failed).

    Raises
    ------
    ConfigError
        If the parameters are missing or invalid.
    MissingCredentialsError
        If the CDSE credentials are missing.
    """
    cli_values = {"safe_dir": args.safe_dir, "orbit_dir": args.orbit_dir}
    values = merge_cli_overrides(flatten_config(load_yaml_config(args.config)), cli_values)
    settings = build_orbit_settings(values)

    settings.orbit_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== Effective parameters ===")
    logger.info("safe_dir  : %s", settings.safe_dir)
    logger.info("orbit_dir : %s", settings.orbit_dir)
    logger.info("============================")

    products = find_safe_products(settings.safe_dir)
    logger.info("Found %d SAFE products", len(products))

    access_key, secret_key = load_credentials()
    s3_client = create_s3_client(access_key, secret_key, max_connections=S3_CONNECTIONS)
    session = create_http_session()
    local_orbits = list_local_orbits(settings.orbit_dir)

    counts = {STATUS_SKIPPED: 0, STATUS_DOWNLOADED: 0, STATUS_FAILED: 0}

    for index, product in enumerate(products, start=1):
        try:
            scene = parse_scene_name(product.name)
            status, message = ensure_orbit_for_scene(
                session, s3_client, scene, settings.orbit_dir, local_orbits
            )
        except (ValueError, requests.RequestException, BotoCoreError, ClientError) as error:
            status, message = STATUS_FAILED, str(error)

        counts[status] += 1
        log = logger.error if status == STATUS_FAILED else logger.info
        log("[%d/%d] %s %s", index, len(products), status.upper(), product.name)
        log("    %s", message)

    logger.info("=== Orbit download summary ===")
    logger.info("SAFE products : %d", len(products))
    logger.info("Downloaded    : %d", counts[STATUS_DOWNLOADED])
    logger.info("Skipped       : %d", counts[STATUS_SKIPPED])
    logger.info("Failed        : %d", counts[STATUS_FAILED])
    logger.info("==============================")

    return 1 if counts[STATUS_FAILED] else 0


def main():
    """Command-line entry point."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)

    try:
        sys.exit(run(parse_arguments()))
    except (ConfigError, MissingCredentialsError) as error:
        logger.error("%s", error)
        sys.exit(2)


if __name__ == "__main__":
    main()
