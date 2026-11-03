"""
Find and download Sentinel-1 orbit files (.EOF) from CDSE.

Two kinds of orbit files exist:

- AUX_POEORB (precise orbit): best accuracy, published about 20 days
  after the acquisition. Always preferred.
- AUX_RESORB (restituted orbit): available within hours, less accurate.
  Used only when no precise orbit exists yet. CDSE removes old RESORB
  files once the precise orbit is available.

An orbit file fits a scene when its validity period covers the whole
acquisition (start to stop) plus a safety margin.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from cdse import HTTP_TIMEOUT, ODATA_PRODUCTS_URL, split_s3_url

logger = logging.getLogger(__name__)

PRECISE = "POEORB"
RESTITUTED = "RESORB"

# Required orbit coverage before the scene start and after the scene stop.
# Processors interpolate the orbit around the acquisition, so the orbit
# must reach a little beyond the scene itself.
COVERAGE_MARGIN = timedelta(seconds=60)

NAME_TIME_FORMAT = "%Y%m%dT%H%M%S"
ODATA_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"

# S1A_IW_SLC__1SDV_20210529T165025_20210529T165052_027122_033D69_8607(.SAFE)
SCENE_NAME_PATTERN = re.compile(
    r"^(?P<mission>S1[A-D])_\w+?_(?P<start>\d{8}T\d{6})_(?P<stop>\d{8}T\d{6})_\d{6}_"
)

# S1A_OPER_AUX_POEORB_OPOD_20210618T121711_V20210528T225942_20210530T005942.EOF
ORBIT_NAME_PATTERN = re.compile(
    r"^(?P<mission>S1[A-D])_OPER_AUX_(?P<orbit_type>POEORB|RESORB)_OPOD_"
    r"(?P<created>\d{8}T\d{6})_V(?P<valid_from>\d{8}T\d{6})_(?P<valid_to>\d{8}T\d{6})\.EOF$"
)


@dataclass(frozen=True)
class SceneTiming:
    """Satellite and acquisition period of one SLC scene."""

    name: str
    mission: str  # "S1A", "S1B", "S1C" or "S1D"
    start: datetime
    stop: datetime


@dataclass(frozen=True)
class OrbitFile:
    """An orbit file, either local or on CDSE."""

    name: str
    mission: str
    orbit_type: str  # PRECISE or RESTITUTED
    created: datetime
    valid_from: datetime
    valid_to: datetime

    def covers(self, scene):
        """Return True if this orbit belongs to the scene's satellite and covers it with margin."""
        return (
            self.mission == scene.mission
            and self.valid_from <= scene.start - COVERAGE_MARGIN
            and self.valid_to >= scene.stop + COVERAGE_MARGIN
        )


def parse_scene_name(name):
    """
    Read satellite and acquisition times from an SLC product name.

    Parameters
    ----------
    name : str
        Product name, with or without `.SAFE`.

    Returns
    -------
    SceneTiming

    Raises
    ------
    ValueError
        If the name is not a Sentinel-1 product name.
    """
    match = SCENE_NAME_PATTERN.match(name)
    if not match:
        raise ValueError(f"Not a Sentinel-1 product name: {name}")

    return SceneTiming(
        name=name,
        mission=match["mission"],
        start=datetime.strptime(match["start"], NAME_TIME_FORMAT),
        stop=datetime.strptime(match["stop"], NAME_TIME_FORMAT),
    )


def parse_orbit_name(name):
    """
    Read satellite, type and validity period from an orbit file name.

    Parameters
    ----------
    name : str
        File name ending in `.EOF`.

    Returns
    -------
    OrbitFile or None
        None if the name is not a Sentinel-1 POEORB/RESORB file name.
    """
    match = ORBIT_NAME_PATTERN.match(name)
    if not match:
        return None

    return OrbitFile(
        name=name,
        mission=match["mission"],
        orbit_type=match["orbit_type"],
        created=datetime.strptime(match["created"], NAME_TIME_FORMAT),
        valid_from=datetime.strptime(match["valid_from"], NAME_TIME_FORMAT),
        valid_to=datetime.strptime(match["valid_to"], NAME_TIME_FORMAT),
    )


def list_local_orbits(orbit_dir):
    """
    List all orbit files in a folder (not recursive).

    Returns
    -------
    list of OrbitFile
        Files whose name is not a valid orbit file name are ignored.
    """
    orbits = (parse_orbit_name(path.name) for path in Path(orbit_dir).glob("*.EOF"))
    return [orbit for orbit in orbits if orbit is not None]


def best_local_orbit(scene, local_orbits):
    """
    Pick the best local orbit file for a scene.

    Precise orbits beat restituted ones; among equals the newest file wins.

    Returns
    -------
    OrbitFile or None
        None if no local file covers the scene.
    """
    candidates = [orbit for orbit in local_orbits if orbit.covers(scene)]
    if not candidates:
        return None

    return max(candidates, key=lambda orbit: (orbit.orbit_type == PRECISE, orbit.created))


def query_cdse_orbit(session, scene, orbit_type):
    """
    Ask the CDSE catalogue for the newest orbit file of a type that covers a scene.

    Parameters
    ----------
    session : requests.Session
    scene : SceneTiming
    orbit_type : str
        PRECISE or RESTITUTED.

    Returns
    -------
    tuple of (OrbitFile, str) or None
        The orbit file and its S3 path, or None if CDSE has none.

    Raises
    ------
    requests.HTTPError
        If the catalogue returns an error after all retries.
    """
    required_from = (scene.start - COVERAGE_MARGIN).strftime(ODATA_TIME_FORMAT)
    required_to = (scene.stop + COVERAGE_MARGIN).strftime(ODATA_TIME_FORMAT)

    odata_filter = (
        "Collection/Name eq 'SENTINEL-1' "
        f"and startswith(Name,'{scene.mission}_OPER_AUX_{orbit_type}_') "
        f"and ContentDate/Start le {required_from} "
        f"and ContentDate/End ge {required_to}"
    )
    params = {"$filter": odata_filter, "$orderby": "PublicationDate desc", "$top": 1}

    response = session.get(ODATA_PRODUCTS_URL, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    products = response.json().get("value", [])
    if not products:
        return None

    orbit = parse_orbit_name(products[0]["Name"])
    if orbit is None:
        logger.warning("Unexpected orbit file name from CDSE: %s", products[0]["Name"])
        return None

    return orbit, products[0]["S3Path"]


def download_orbit(s3_client, s3_path, orbit_dir):
    """
    Download one orbit file from CDSE S3 into `orbit_dir`.

    Parameters
    ----------
    s3_client : botocore.client.S3
    s3_path : str
        S3 path from the catalogue, e.g. "/eodata/Sentinel-1/AUX/...EOF".
    orbit_dir : Path

    Returns
    -------
    Path
        Path of the downloaded file.
    """
    bucket, key = split_s3_url(s3_path)
    target = Path(orbit_dir) / Path(key).name

    # boto3 downloads into a temporary file and renames it when done,
    # so a broken download never leaves a half-written .EOF file.
    s3_client.download_file(bucket, key, str(target))
    return target
