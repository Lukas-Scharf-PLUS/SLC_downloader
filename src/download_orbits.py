#!/usr/bin/env python3
import argparse
import os
import re
from datetime import datetime
from pathlib import Path

import s1_orbits
import yaml
import subprocess


def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def orbit_exists(scene_name, orbit_dir):

    scene_sat = scene_name[:3]  # S1A or S1B

    acq_time = datetime.strptime(
        scene_name.split("_")[5],
        "%Y%m%dT%H%M%S"
    )

    for orbit in orbit_dir.glob("*.EOF"):

        # must be same satellite
        if not orbit.name.startswith(scene_sat):
            continue

        m = re.search(
            r"V(\d{8}T\d{6})_(\d{8}T\d{6})",
            orbit.name
        )

        if not m:
            continue

        orbit_start = datetime.strptime(
            m.group(1),
            "%Y%m%dT%H%M%S"
        )

        orbit_end = datetime.strptime(
            m.group(2),
            "%Y%m%dT%H%M%S"
        )

        if orbit_start <= acq_time <= orbit_end:
            return orbit

    return None



def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        default=os.getenv(
            "CONFIG_PATH",
            "configs/vienna_2020.yaml"
        )
    )

    parser.add_argument(
        "--safe_dir",
        help="Directory containing SAFE folders"
    )

    parser.add_argument(
        "--orbit_dir",
        help="Directory where orbit files will be stored"
    )

    args = parser.parse_args()

    # -------------------------
    # load config
    # -------------------------

    config = load_config(args.config)

    safe_dir = Path(
        args.safe_dir
        or config["orbits"]["safe_dir"]
    )

    orbit_dir = Path(
        args.orbit_dir
        or config["orbits"]["orbit_dir"]
    )

    orbit_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    # -------------------------
    # effective parameters
    # -------------------------

    print("\n=== Effective Parameters ===")
    print(f"safe_dir   : {safe_dir}")
    print(f"orbit_dir  : {orbit_dir}")
    print("============================\n")

    # -------------------------
    # find SAFE files
    # -------------------------

    safe_files = sorted(
        safe_dir.rglob("*.SAFE")
    )

    print(
        f"Found {len(safe_files)} SAFE files\n"
    )

    downloaded = 0
    skipped = 0
    failed = 0

    # -------------------------
    # download orbits
    # -------------------------

    for i, scene in enumerate(
        safe_files,
        start=1
    ):

        try:

            existing = orbit_exists(
                scene.stem,
                orbit_dir
            ) 

            if existing:

                print(
                    f"[{i}/{len(safe_files)}] "
                    f"SKIP {scene.name}"
                )
                print(
                    f"    Existing orbit: "
                    f"{existing.name}\n"
                )

                skipped += 1
                continue

            orbit_file = s1_orbits.fetch_for_scene(
                scene.stem,
                dir=orbit_dir
            )

            print(
                f"[{i}/{len(safe_files)}] "
                f"DOWNLOADED {scene.name}"
            )
            print(
                f"    Orbit: "
                f"{Path(orbit_file).name}\n"
            )

            downloaded += 1

        except Exception as e:

            print(
                f"[{i}/{len(safe_files)}] "
                f"FAILED {scene.name}"
            )
            print(f"    {e}\n")

            failed += 1

    # -------------------------
    # summary
    # -------------------------

    print("\n=== Orbit Download Summary ===")
    print(f"SAFE files scanned : {len(safe_files)}")
    print(f"Downloaded         : {downloaded}")
    print(f"Skipped            : {skipped}")
    print(f"Failed             : {failed}")
    print("================================\n")

    #print("\n=== PVC CONTENT AFTER ORBIT DOWNLOAD ===")
    #subprocess.run(["find", "/data", "-maxdepth", "4"])


if __name__ == "__main__":
    main()