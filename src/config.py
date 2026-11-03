"""
Configuration handling for the downloaders.

Every parameter can come from a YAML config file, from the command line,
or from both. Command-line values always win over config values. After
merging, the values are validated so that mistakes are reported before
anything is searched or downloaded.
"""

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

ORBIT_STATES = ("ascending", "descending")
INSTRUMENT_MODES = ("IW", "EW", "SM", "WV")
POLARIZATIONS = ("VV", "VH", "HH", "HV")
MAX_RELATIVE_ORBIT = 175  # Sentinel-1 has 175 relative orbits per 12-day cycle

DEFAULT_POLARIZATION = "VV"
# CDSE allows 4 concurrent S3 connections for general users. Downloading 4
# files with one connection each uses the full quota (4 x 20 MB/s).
DEFAULT_MAX_CONNECTIONS = 4
DEFAULT_MAX_THREADS = 4
DEFAULT_THREADS_PER_FILE = 1
DEFAULT_MAX_RETRIES = 3

# Keys that belong to the "download" / "orbits" sections of the config file.
# Everything else is a top-level search parameter.
DOWNLOAD_KEYS = ("base_path", "max_threads", "threads_per_file", "max_connections", "max_retries")
ORBIT_KEYS = ("safe_dir", "orbit_dir")


class ConfigError(ValueError):
    """Raised when the merged configuration is missing values or is invalid."""


@dataclass(frozen=True)
class SlcSettings:
    """Validated parameters for searching and downloading SLC products."""

    orbit_state: str
    relative_orbit: int
    instrument_mode: str
    polarization: str
    start_date: date
    end_date: date  # inclusive
    bbox: tuple  # (west, south, east, north) in degrees
    base_path: Path
    max_threads: int
    threads_per_file: int
    max_connections: int  # upper limit for max_threads * threads_per_file
    max_retries: int


@dataclass(frozen=True)
class OrbitSettings:
    """Validated parameters for downloading orbit files."""

    safe_dir: Path
    orbit_dir: Path


def load_yaml_config(path):
    """
    Read a YAML config file.

    Parameters
    ----------
    path : str or Path or None
        Path to the YAML file. `None` means "no config file".

    Returns
    -------
    dict
        The parsed config, or an empty dict if no path was given.

    Raises
    ------
    ConfigError
        If the file does not exist or does not contain a YAML mapping.
    """
    if path is None:
        return {}

    path = Path(path).expanduser()
    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f) or {}

    if not isinstance(config, dict):
        raise ConfigError(f"Config file must contain key/value pairs: {path}")

    return config


def flatten_config(config):
    """
    Turn the nested config file layout into one flat dict.

    The `download:` and `orbits:` sections are merged into the top level so
    that each value can be looked up by its command-line name.

    Parameters
    ----------
    config : dict
        Config as read from YAML.

    Returns
    -------
    dict
        Flat mapping of parameter name to value.
    """
    flat = {
        key: value
        for key, value in config.items()
        if key not in ("download", "orbits")
    }

    for section, keys in (("download", DOWNLOAD_KEYS), ("orbits", ORBIT_KEYS)):
        section_values = config.get(section) or {}
        for key in keys:
            if key in section_values:
                flat[key] = section_values[key]

    return flat


def merge_cli_overrides(config_values, cli_values):
    """
    Overlay command-line values on top of config values.

    Parameters
    ----------
    config_values : dict
        Flat config values (see `flatten_config`).
    cli_values : dict
        Values from argparse. `None` means "not given on the command line".

    Returns
    -------
    dict
        Merged values; command-line values take precedence.
    """
    merged = dict(config_values)
    for key, value in cli_values.items():
        if value is not None:
            merged[key] = value
    return merged


def build_slc_settings(values):
    """
    Validate merged values and build the SLC download settings.

    Parameters
    ----------
    values : dict
        Merged config and command-line values.

    Returns
    -------
    SlcSettings

    Raises
    ------
    ConfigError
        If a required value is missing or a value is invalid.
    """
    _require(values, ("orbit_state", "relative_orbit", "instrument_mode",
                      "start_date", "end_date", "bbox", "base_path"))

    orbit_state = str(values["orbit_state"]).lower()
    if orbit_state not in ORBIT_STATES:
        raise ConfigError(f"orbit_state must be one of {ORBIT_STATES}, got '{orbit_state}'")

    relative_orbit = _parse_int(values["relative_orbit"], "relative_orbit")
    if not 1 <= relative_orbit <= MAX_RELATIVE_ORBIT:
        raise ConfigError(f"relative_orbit must be between 1 and {MAX_RELATIVE_ORBIT}")

    instrument_mode = str(values["instrument_mode"]).upper()
    if instrument_mode not in INSTRUMENT_MODES:
        raise ConfigError(f"instrument_mode must be one of {INSTRUMENT_MODES}")

    polarization = str(values.get("polarization") or DEFAULT_POLARIZATION).upper()
    if polarization not in POLARIZATIONS:
        raise ConfigError(f"polarization must be one of {POLARIZATIONS}")

    start_date = _parse_date(values["start_date"], "start_date")
    end_date = _parse_date(values["end_date"], "end_date")
    if start_date > end_date:
        raise ConfigError("start_date must not be after end_date")

    max_threads = _parse_positive_int(values.get("max_threads", DEFAULT_MAX_THREADS), "max_threads")
    threads_per_file = _parse_positive_int(
        values.get("threads_per_file", DEFAULT_THREADS_PER_FILE), "threads_per_file"
    )
    max_connections = _parse_positive_int(
        values.get("max_connections", DEFAULT_MAX_CONNECTIONS), "max_connections"
    )
    # Going over the CDSE connection limit does not make downloads faster;
    # CDSE rejects the extra connections with HTTP 429 and the run fails.
    if max_threads * threads_per_file > max_connections:
        raise ConfigError(
            f"max_threads ({max_threads}) x threads_per_file ({threads_per_file}) = "
            f"{max_threads * threads_per_file} connections, but max_connections is "
            f"{max_connections} (the CDSE limit for general users is 4). "
            "Lower max_threads or threads_per_file."
        )

    return SlcSettings(
        orbit_state=orbit_state,
        relative_orbit=relative_orbit,
        instrument_mode=instrument_mode,
        polarization=polarization,
        start_date=start_date,
        end_date=end_date,
        bbox=parse_bbox(values["bbox"]),
        base_path=_parse_path(values["base_path"]),
        max_threads=max_threads,
        threads_per_file=threads_per_file,
        max_connections=max_connections,
        max_retries=_parse_positive_int(values.get("max_retries", DEFAULT_MAX_RETRIES), "max_retries"),
    )


def build_orbit_settings(values):
    """
    Validate merged values and build the orbit download settings.

    Parameters
    ----------
    values : dict
        Merged config and command-line values.

    Returns
    -------
    OrbitSettings

    Raises
    ------
    ConfigError
        If `safe_dir` or `orbit_dir` is missing, or `safe_dir` does not exist.
    """
    _require(values, ("safe_dir", "orbit_dir"))

    safe_dir = _parse_path(values["safe_dir"])
    if not safe_dir.is_dir():
        raise ConfigError(f"safe_dir does not exist: {safe_dir}")

    return OrbitSettings(safe_dir=safe_dir, orbit_dir=_parse_path(values["orbit_dir"]))


def parse_bbox(value):
    """
    Parse a bounding box given as a list or as a string.

    Strings may separate the numbers with spaces and/or commas,
    e.g. "16.18 48.11 16.58 48.33" or "16.18,48.11,16.58,48.33".

    Parameters
    ----------
    value : str or list
        Four numbers: west, south, east, north (degrees).

    Returns
    -------
    tuple of float
        (west, south, east, north)

    Raises
    ------
    ConfigError
        If there are not exactly four numbers or the box is not valid.
    """
    parts = value.replace(",", " ").split() if isinstance(value, str) else list(value)

    try:
        bbox = tuple(float(x) for x in parts)
    except (TypeError, ValueError):
        raise ConfigError(f"bbox must contain numbers, got {value!r}") from None

    if len(bbox) != 4:
        raise ConfigError("bbox must contain exactly 4 values: WEST SOUTH EAST NORTH")

    west, south, east, north = bbox
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ConfigError(
            f"bbox {bbox} is not valid: expected WEST < EAST within ±180 "
            "and SOUTH < NORTH within ±90"
        )

    return bbox


def _require(values, keys):
    """Raise ConfigError listing every key in `keys` that has no value."""
    missing = [key for key in keys if values.get(key) in (None, "")]
    if missing:
        raise ConfigError(
            "Missing required parameter(s): " + ", ".join(missing)
            + ". Set them in the config file or pass them as --<name>."
        )


def _parse_date(value, name):
    """Accept a date object (YAML parses unquoted dates) or an ISO string."""
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        raise ConfigError(f"{name} must be a date like 2021-01-31, got '{value}'") from None


def _parse_int(value, name):
    """Convert to int or raise a ConfigError that names the parameter."""
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{name} must be a whole number, got '{value}'") from None


def _parse_positive_int(value, name):
    """Convert to int and make sure it is at least 1."""
    number = _parse_int(value, name)
    if number < 1:
        raise ConfigError(f"{name} must be at least 1, got {number}")
    return number


def _parse_path(value):
    """Expand environment variables (e.g. $HOME) and `~` in a path."""
    return Path(os.path.expandvars(str(value))).expanduser()
