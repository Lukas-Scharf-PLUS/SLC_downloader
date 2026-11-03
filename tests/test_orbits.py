"""Tests for orbit name parsing, coverage checks and choosing POEORB vs RESORB."""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from download_orbits import (STATUS_DOWNLOADED, STATUS_FAILED, STATUS_SKIPPED,
                             ensure_orbit_for_scene)
from orbits import (PRECISE, RESTITUTED, best_local_orbit, list_local_orbits,
                    parse_orbit_name, parse_scene_name)

SCENE_NAME = "S1C_IW_SLC__1SDV_20251001T050000_20251001T050027_004000_007D00_ABCD.SAFE"
POEORB = "S1C_OPER_AUX_POEORB_OPOD_20251021T071108_V20250930T225942_20251002T005942.EOF"
RESORB = "S1C_OPER_AUX_RESORB_OPOD_20251001T083350_V20251001T041656_20251001T081036.EOF"
# Ends 30 s after the scene stop, i.e. inside the required 60 s margin.
RESORB_TOO_SHORT = "S1C_OPER_AUX_RESORB_OPOD_20251001T060000_V20251001T030000_20251001T050057.EOF"
OTHER_SATELLITE = "S1A_OPER_AUX_POEORB_OPOD_20251021T071108_V20250930T225942_20251002T005942.EOF"


class ParsingTests(unittest.TestCase):
    def test_scene_names_of_all_satellites(self):
        for mission in ("S1A", "S1B", "S1C", "S1D"):
            with self.subTest(mission=mission):
                scene = parse_scene_name(mission + SCENE_NAME[3:])
                self.assertEqual(scene.mission, mission)
                self.assertEqual(scene.start, datetime(2025, 10, 1, 5, 0, 0))
                self.assertEqual(scene.stop, datetime(2025, 10, 1, 5, 0, 27))

    def test_other_product_types_parse_too(self):
        scene = parse_scene_name("S1A_EW_SLC__1SDH_20200101T000000_20200101T000100_030000_036000_0000")
        self.assertEqual(scene.mission, "S1A")

    def test_invalid_scene_name_raises(self):
        with self.assertRaises(ValueError):
            parse_scene_name("S2A_MSIL1C_20200101T000000_N0208_R000_T33UXP_20200101T000000.SAFE")

    def test_orbit_name(self):
        orbit = parse_orbit_name(POEORB)
        self.assertEqual((orbit.mission, orbit.orbit_type), ("S1C", PRECISE))
        self.assertEqual(orbit.valid_to, datetime(2025, 10, 2, 0, 59, 42))
        self.assertIsNone(parse_orbit_name("notes.EOF"))


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.scene = parse_scene_name(SCENE_NAME)

    def test_covers_requires_same_satellite_and_margin(self):
        self.assertTrue(parse_orbit_name(POEORB).covers(self.scene))
        self.assertTrue(parse_orbit_name(RESORB).covers(self.scene))
        self.assertFalse(parse_orbit_name(RESORB_TOO_SHORT).covers(self.scene))
        self.assertFalse(parse_orbit_name(OTHER_SATELLITE).covers(self.scene))

    def test_precise_is_preferred(self):
        resorb = parse_orbit_name("S1C_OPER_AUX_RESORB_OPOD_20251001T083350_V20251001T030000_20251001T070000.EOF")
        best = best_local_orbit(self.scene, [resorb, parse_orbit_name(POEORB)])
        self.assertEqual(best.orbit_type, PRECISE)


# A RESORB that does cover the scene (03:00-07:00).
RESORB_COVERING = "S1C_OPER_AUX_RESORB_OPOD_20251001T083350_V20251001T030000_20251001T070000.EOF"


class FakeCatalogue:
    """Stands in for `query_cdse_orbit` results via a fake session."""

    def __init__(self, available):
        self.available = available  # orbit_type -> file name

    def get(self, url, params=None, timeout=None):
        orbit_type = PRECISE if "POEORB" in params["$filter"] else RESTITUTED
        name = self.available.get(orbit_type)
        value = [{"Name": name, "S3Path": f"/eodata/Sentinel-1/AUX/AUX_{orbit_type}/{name}"}] if name else []
        return FakeResponse({"value": value})


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class FakeS3:
    def __init__(self):
        self.downloaded = []

    def download_file(self, bucket, key, filename, Config=None):
        self.downloaded.append(Path(key).name)
        Path(filename).write_text("orbit")


class EnsureOrbitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.orbit_dir = Path(self.tmp.name)
        self.scene = parse_scene_name(SCENE_NAME)

    def tearDown(self):
        self.tmp.cleanup()

    def ensure(self, available, s3=None):
        s3 = s3 or FakeS3()
        local = list_local_orbits(self.orbit_dir)
        return ensure_orbit_for_scene(FakeCatalogue(available), s3, self.scene, self.orbit_dir, local)

    def test_downloads_precise_orbit(self):
        status, _ = self.ensure({PRECISE: POEORB, RESTITUTED: RESORB_COVERING})
        self.assertEqual(status, STATUS_DOWNLOADED)
        self.assertTrue((self.orbit_dir / POEORB).exists())

    def test_falls_back_to_restituted_orbit(self):
        status, message = self.ensure({RESTITUTED: RESORB_COVERING})
        self.assertEqual(status, STATUS_DOWNLOADED)
        self.assertIn(RESTITUTED, message)

    def test_existing_precise_orbit_is_skipped(self):
        (self.orbit_dir / POEORB).write_text("orbit")
        s3 = FakeS3()
        status, _ = self.ensure({PRECISE: POEORB}, s3)
        self.assertEqual(status, STATUS_SKIPPED)
        self.assertEqual(s3.downloaded, [])

    def test_restituted_orbit_is_replaced_by_precise_one(self):
        (self.orbit_dir / RESORB_COVERING).write_text("orbit")
        status, _ = self.ensure({PRECISE: POEORB})
        self.assertEqual(status, STATUS_DOWNLOADED)
        self.assertTrue((self.orbit_dir / POEORB).exists())
        self.assertFalse((self.orbit_dir / RESORB_COVERING).exists())

    def test_existing_restituted_orbit_is_kept_when_no_precise_exists(self):
        (self.orbit_dir / RESORB_COVERING).write_text("orbit")
        status, _ = self.ensure({RESTITUTED: RESORB_COVERING})
        self.assertEqual(status, STATUS_SKIPPED)

    def test_no_orbit_available_fails(self):
        status, _ = self.ensure({})
        self.assertEqual(status, STATUS_FAILED)


if __name__ == "__main__":
    unittest.main()
