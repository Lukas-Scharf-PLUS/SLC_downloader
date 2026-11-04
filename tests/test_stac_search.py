"""Tests for the STAC search: pagination, S3 locations and filtering."""

import unittest
from datetime import date

from config import build_slc_settings
from stac_search import (build_search_payload, fetch_all_items,
                         find_safe_prefix, items_to_scenes)

PRODUCT_ID = "S1A_IW_SLC__1SDV_20210529T165025_20210529T165052_027122_033D69_8607"
SAFE_PREFIX = f"Sentinel-1/SAR/SLC/2021/05/29/{PRODUCT_ID}.SAFE/"


def make_item(item_id=PRODUCT_ID, polarizations=("VV", "VH"), when="2021-05-29T16:50:25Z"):
    """Build a minimal STAC item like the ones CDSE returns."""
    return {
        "id": item_id,
        "properties": {"datetime": when, "sar:polarizations": list(polarizations)},
        "assets": {
            "thumbnail": {"href": "https://example.com/thumb.png"},
            "iw1-vv": {"href": f"s3://eodata/Sentinel-1/SAR/SLC/2021/05/29/{item_id}.SAFE/measurement/x.tiff"},
        },
    }


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class FakeSession:
    """Returns prepared pages in order and records every request."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.requests = []

    def request(self, method, url, json=None, timeout=None):
        self.requests.append({"method": method, "url": url, "json": json})
        return FakeResponse(self.pages.pop(0))


class PaginationTests(unittest.TestCase):
    def test_post_next_link_sends_its_body(self):
        next_body = {"collections": ["sentinel-1-slc"], "token": "next:abc"}
        session = FakeSession([
            {"features": [make_item("A")],
             "links": [{"rel": "next", "method": "POST", "href": "https://stac/next", "body": next_body}]},
            {"features": [make_item("B")], "links": []},
        ])

        items = fetch_all_items(session, {"collections": ["sentinel-1-slc"]})

        self.assertEqual([item["id"] for item in items], ["A", "B"])
        self.assertEqual(session.requests[1], {"method": "POST", "url": "https://stac/next", "json": next_body})

    def test_get_next_link_is_followed(self):
        session = FakeSession([
            {"features": [], "links": [{"rel": "next", "href": "https://stac/next?token=1"}]},
            {"features": [], "links": []},
        ])
        fetch_all_items(session, {})
        self.assertEqual(session.requests[1]["method"], "GET")


class SceneConversionTests(unittest.TestCase):
    def test_s3_prefix_is_taken_from_assets(self):
        self.assertEqual(find_safe_prefix(make_item(), f"{PRODUCT_ID}.SAFE"), ("eodata", SAFE_PREFIX))

    def test_item_without_s3_asset_has_no_prefix(self):
        item = make_item()
        item["assets"] = {"thumbnail": {"href": "https://example.com/thumb.png"}}
        self.assertIsNone(find_safe_prefix(item, f"{PRODUCT_ID}.SAFE"))

    def test_polarization_filter_duplicates_and_sorting(self):
        later = make_item("S1C_IW_SLC__1SDV_20250101T000000_20250101T000027_000001_000001_AAAA",
                          when="2025-01-01T00:00:00Z")
        hh_only = make_item("S1A_IW_SLC__1SDH_20210101T000000_20210101T000027_000001_000001_BBBB",
                            polarizations=("HH", "HV"))

        scenes = items_to_scenes([later, make_item(), make_item(), hh_only], "VV")

        self.assertEqual([s.product_name for s in scenes], [f"{PRODUCT_ID}.SAFE", f"{later['id']}.SAFE"])
        self.assertEqual(scenes[0].prefix, SAFE_PREFIX)

    def test_platform_filter(self):
        s1c = make_item("S1C_IW_SLC__1SDV_20250101T000000_20250101T000027_000001_000001_AAAA",
                        when="2025-01-01T00:00:00Z")
        items = [make_item(), s1c]  # make_item() is an S1A product

        only_c = items_to_scenes(items, "VV", ("S1C",))
        both = items_to_scenes(items, "VV", ("S1A", "S1C"))
        no_filter = items_to_scenes(items, "VV")

        self.assertEqual([s.product_name[:3] for s in only_c], ["S1C"])
        self.assertEqual([s.product_name[:3] for s in both], ["S1A", "S1C"])
        self.assertEqual(len(no_filter), 2)


class PayloadTests(unittest.TestCase):
    def test_end_date_is_inclusive(self):
        settings = build_slc_settings({
            "orbit_state": "ascending", "relative_orbit": 73, "instrument_mode": "IW",
            "start_date": date(2020, 6, 1), "end_date": date(2020, 6, 30),
            "bbox": [16, 48, 17, 49], "base_path": "/data",
        })
        payload = build_search_payload(settings)
        self.assertEqual(payload["datetime"], "2020-06-01T00:00:00Z/2020-06-30T23:59:59.999Z")
        self.assertEqual(payload["query"]["sat:relative_orbit"], {"eq": 73})


if __name__ == "__main__":
    unittest.main()
