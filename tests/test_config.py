"""Tests for merging and validating config file and command-line values."""

import tempfile
import unittest
from datetime import date
from pathlib import Path

from config import (ConfigError, build_orbit_settings, build_slc_settings,
                    flatten_config, load_yaml_config, merge_cli_overrides,
                    parse_bbox)

VALID_VALUES = {
    "orbit_state": "Ascending",
    "relative_orbit": 73,
    "instrument_mode": "iw",
    "start_date": "2020-06-01",
    "end_date": "2020-06-30",
    "bbox": [16.18, 48.11, 16.58, 48.33],
    "base_path": "/data",
}


class FlattenAndMergeTests(unittest.TestCase):
    def test_sections_are_flattened(self):
        config = {"orbit_state": "ascending",
                  "download": {"base_path": "/data", "max_threads": 6},
                  "orbits": {"orbit_dir": "/orbits"}}
        self.assertEqual(
            flatten_config(config),
            {"orbit_state": "ascending", "base_path": "/data", "max_threads": 6, "orbit_dir": "/orbits"},
        )

    def test_cli_values_override_config_but_none_does_not(self):
        merged = merge_cli_overrides({"max_threads": 6, "base_path": "/data"},
                                     {"max_threads": 12, "base_path": None})
        self.assertEqual(merged, {"max_threads": 12, "base_path": "/data"})

    def test_yaml_file_is_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.yaml"
            path.write_text("relative_orbit: 73\ndownload:\n  base_path: /data\n")
            self.assertEqual(load_yaml_config(path), {"relative_orbit": 73, "download": {"base_path": "/data"}})

    def test_missing_yaml_file_raises(self):
        with self.assertRaises(ConfigError):
            load_yaml_config("/does/not/exist.yaml")

    def test_no_config_path_gives_empty_config(self):
        self.assertEqual(load_yaml_config(None), {})


class SlcSettingsTests(unittest.TestCase):
    def test_valid_values_are_normalised_and_defaults_applied(self):
        settings = build_slc_settings(VALID_VALUES)
        self.assertEqual(settings.orbit_state, "ascending")
        self.assertEqual(settings.instrument_mode, "IW")
        self.assertEqual(settings.polarization, "VV")
        self.assertEqual(settings.end_date, date(2020, 6, 30))
        self.assertEqual(settings.max_threads, 4)
        self.assertEqual(settings.max_retries, 3)

    def test_yaml_date_objects_are_accepted(self):
        settings = build_slc_settings({**VALID_VALUES, "start_date": date(2020, 6, 1)})
        self.assertEqual(settings.start_date, date(2020, 6, 1))

    def test_missing_values_are_all_listed(self):
        with self.assertRaises(ConfigError) as context:
            build_slc_settings({"orbit_state": "ascending"})
        self.assertIn("relative_orbit", str(context.exception))
        self.assertIn("bbox", str(context.exception))

    def test_invalid_values_raise(self):
        invalid_cases = [
            {"orbit_state": "north"},
            {"relative_orbit": 0},
            {"relative_orbit": 176},
            {"instrument_mode": "XX"},
            {"start_date": "2020-13-01"},
            {"start_date": "2020-07-01", "end_date": "2020-06-01"},
            {"max_threads": 0},
        ]
        for override in invalid_cases:
            with self.subTest(override=override), self.assertRaises(ConfigError):
                build_slc_settings({**VALID_VALUES, **override})

    def test_environment_variables_in_paths_are_expanded(self):
        settings = build_slc_settings({**VALID_VALUES, "base_path": "$HOME/data"})
        self.assertEqual(settings.base_path, Path.home() / "data")


class BboxTests(unittest.TestCase):
    def test_string_with_spaces_or_commas(self):
        self.assertEqual(parse_bbox("16.18 48.11 16.58 48.33"), (16.18, 48.11, 16.58, 48.33))
        self.assertEqual(parse_bbox("16.18,48.11, 16.58,48.33"), (16.18, 48.11, 16.58, 48.33))

    def test_invalid_boxes_raise(self):
        for value in ("1 2 3", "a b c d", [16.58, 48.11, 16.18, 48.33], [0, 95, 1, 96]):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                parse_bbox(value)


class OrbitSettingsTests(unittest.TestCase):
    def test_safe_dir_must_exist(self):
        with self.assertRaises(ConfigError):
            build_orbit_settings({"safe_dir": "/does/not/exist", "orbit_dir": "/tmp/x"})

    def test_both_dirs_are_required(self):
        with self.assertRaises(ConfigError):
            build_orbit_settings({"safe_dir": "/tmp"})


if __name__ == "__main__":
    unittest.main()
