"""
Search Sentinel-1 SLC products in the CDSE STAC catalogue.

The STAC items already contain the S3 location of every file, so no
extra catalogue (OData) request per product is needed.
"""

import logging
from dataclasses import dataclass

from cdse import HTTP_TIMEOUT, STAC_SEARCH_URL, split_s3_url

logger = logging.getLogger(__name__)

SLC_COLLECTION = "sentinel-1-slc"
RESULTS_PER_PAGE = 100


@dataclass(frozen=True)
class Scene:
    """One SLC product found by the search."""

    product_name: str  # e.g. "S1A_IW_SLC__1SDV_..._ABCD.SAFE"
    acquisition_time: str  # ISO timestamp from the STAC item
    bucket: str  # S3 bucket, normally "eodata"
    prefix: str  # S3 key prefix of the SAFE folder, ends with "/"


def build_search_payload(settings):
    """
    Build the STAC search request body from the SLC settings.

    The end date is inclusive: the whole last day is part of the search.

    Parameters
    ----------
    settings : config.SlcSettings

    Returns
    -------
    dict
        JSON body for a STAC `POST /search` request.
    """
    return {
        "collections": [SLC_COLLECTION],
        "bbox": list(settings.bbox),
        "datetime": f"{settings.start_date}T00:00:00Z/{settings.end_date}T23:59:59.999Z",
        "limit": RESULTS_PER_PAGE,
        "query": {
            "sat:orbit_state": {"eq": settings.orbit_state},
            "sat:relative_orbit": {"eq": settings.relative_orbit},
            "sar:instrument_mode": {"eq": settings.instrument_mode},
        },
    }


def fetch_all_items(session, payload):
    """
    Run a STAC search and follow all result pages.

    CDSE returns a "next" link that must be sent as POST together with the
    body it contains (that body carries the page token). Sending the
    original payload or no body at all would repeat or break the search.

    Parameters
    ----------
    session : requests.Session
    payload : dict
        Initial search body.

    Returns
    -------
    list of dict
        All STAC items (GeoJSON features) from all pages.

    Raises
    ------
    requests.HTTPError
        If the catalogue returns an error after all retries.
    """
    items = []
    request = {"method": "POST", "url": STAC_SEARCH_URL, "json": payload}

    while request:
        response = session.request(timeout=HTTP_TIMEOUT, **request)
        response.raise_for_status()
        page = response.json()

        items.extend(page.get("features", []))
        request = _next_page_request(page)

    return items


def _next_page_request(page):
    """Return request arguments for the next page, or None on the last page."""
    next_link = next((link for link in page.get("links", []) if link.get("rel") == "next"), None)
    if next_link is None:
        return None

    method = next_link.get("method", "GET").upper()
    if method == "POST":
        return {"method": "POST", "url": next_link["href"], "json": next_link.get("body")}

    return {"method": "GET", "url": next_link["href"]}


def find_safe_prefix(item, product_name):
    """
    Find the S3 location of the SAFE folder from a STAC item's assets.

    Asset hrefs look like
    `s3://eodata/Sentinel-1/SAR/SLC/2021/05/29/<product>.SAFE/measurement/...`.

    Parameters
    ----------
    item : dict
        STAC item.
    product_name : str
        Product name including `.SAFE`.

    Returns
    -------
    tuple of (str, str) or None
        Bucket and SAFE key prefix (ending in "/"), or None if no asset
        points to S3.
    """
    marker = f"{product_name}/"

    for asset in item.get("assets", {}).values():
        href = asset.get("href", "")
        if href.startswith("s3://") and marker in href:
            bucket, key = split_s3_url(href)
            return bucket, key[: key.index(marker) + len(marker)]

    return None


def items_to_scenes(items, polarization, platforms=None):
    """
    Convert STAC items to Scene objects.

    Items without the requested polarization or from a satellite that is not
    in `platforms` are dropped, as are duplicate items (in case a product
    appears on two pages). Items without an S3 location are reported and
    skipped.

    Parameters
    ----------
    items : list of dict
        STAC items.
    polarization : str
        Required polarization, e.g. "VV".
    platforms : tuple of str, optional
        Satellites to keep, e.g. ("S1A", "S1C"). None keeps all.

    Returns
    -------
    list of Scene
        Sorted by acquisition time.
    """
    scenes = {}
    skipped_by_platform = {}

    for item in items:
        properties = item.get("properties", {})
        if polarization not in properties.get("sar:polarizations", []):
            continue

        product_name = item["id"] if item["id"].endswith(".SAFE") else f"{item['id']}.SAFE"
        if product_name in scenes:
            continue

        # The product name starts with the satellite, e.g. "S1C_IW_SLC__...".
        platform = product_name[:3]
        if platforms is not None and platform not in platforms:
            skipped_by_platform[platform] = skipped_by_platform.get(platform, 0) + 1
            continue

        location = find_safe_prefix(item, product_name)
        if location is None:
            logger.warning("No S3 location in STAC item, skipping: %s", product_name)
            continue

        bucket, prefix = location
        scenes[product_name] = Scene(
            product_name=product_name,
            acquisition_time=properties.get("datetime", ""),
            bucket=bucket,
            prefix=prefix,
        )

    for platform, count in sorted(skipped_by_platform.items()):
        logger.info("Skipped %d %s products (not in platforms)", count, platform)

    return sorted(scenes.values(), key=lambda scene: scene.acquisition_time)


def search_scenes(session, settings):
    """
    Search all SLC products that match the settings.

    Parameters
    ----------
    session : requests.Session
    settings : config.SlcSettings

    Returns
    -------
    list of Scene
    """
    items = fetch_all_items(session, build_search_payload(settings))
    return items_to_scenes(items, settings.polarization, settings.platforms)
