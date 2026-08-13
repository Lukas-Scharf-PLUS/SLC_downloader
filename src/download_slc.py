import os
import argparse
import requests
import boto3
import time
import concurrent.futures
from utils import load_config
#from dotenv import load_dotenv


#load_dotenv("cdse.env")

# -----------------------------
# SAFE EXISTENCE CHECK
# -----------------------------
def safe_exists(base_path, product_name):
    safe_dir = os.path.join(base_path, product_name)
    manifest = os.path.join(safe_dir, "manifest.safe")
    return os.path.exists(manifest)

# -----------------------------
# build a folder name corresponding to some configs
# -----------------------------

def build_run_folder(config):
    orbit = config["orbit_state"][:3].upper()
    rel_orbit = str(config["relative_orbit"]).zfill(3)
    return f"S1_{rel_orbit}_{orbit}"


# -----------------------------
# STAC SEARCH
# -----------------------------
def search_scenes(config: dict):
    payload = {
        "collections": ["sentinel-1-slc"],
        "bbox": config["bbox"],
        "datetime": f"{config['start_date']}T00:00:00Z/{config['end_date']}T00:00:00Z",
        "limit": 100,
        "query": {
            "sat:orbit_state": {"eq": config["orbit_state"].lower()},
            "sat:relative_orbit": {"eq": config["relative_orbit"]},
            "sar:instrument_mode": {"eq": config["instrument_mode"]}
        }
    }

    stac_endpoint = "https://stac.dataspace.copernicus.eu/v1/search"

    items = []
    url = stac_endpoint

    while url:
        r = requests.post(url, json=payload if url == stac_endpoint else None)
        r.raise_for_status()
        data = r.json()

        items.extend(data.get("features", []))

        next_link = next((l for l in data.get("links", []) if l["rel"] == "next"), None)
        url = next_link["href"] if next_link else None

    # VV filter
    items = [
        it for it in items
        if "VV" in it["properties"].get("sar:polarizations", [])
    ]

    scenes = []
    for it in items:
        base_id = it["id"]
        product_name = base_id if base_id.endswith(".SAFE") else f"{base_id}.SAFE"
        p = it["properties"]

        scenes.append({
            "product_name": product_name,
            "datetime": p.get("datetime")
        })

    return scenes


# -----------------------------
# ODATA → S3 PATH
# -----------------------------
def get_s3_path(product_name):
    odata_url = (
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        f"?$filter=Name eq '{product_name}'"
    )

    r = requests.get(odata_url)
    r.raise_for_status()

    product_data = r.json().get("value", [])

    if not product_data:
        print(f"[WARN] Product not found in OData: {product_name}")
        return None, None

    s3_path = product_data[0]["S3Path"]

    parts = s3_path.lstrip("/").split("/", 1)
    bucket = parts[0]
    prefix = parts[1]

    return bucket, prefix


# -----------------------------
# PARALLEL DOWNLOAD FUNCTIONS
# -----------------------------
def download_single_file(s3_key, bucket_name, local_file_path, access_key, secret_key):
    if local_file_path.endswith('/'):
        os.makedirs(local_file_path, exist_ok=True)
        return

    if os.path.exists(local_file_path):
        return

    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

    session = boto3.session.Session()
    s3_client = session.client(
        's3',
        endpoint_url='https://eodata.dataspace.copernicus.eu',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='default'
    )

    s3_client.download_file(bucket_name, s3_key, local_file_path)


def download_product_from_s3_parallel(
    bucket_name, prefix, target_dir, access_key, secret_key, max_threads=4
):
    s3_resource = boto3.resource(
        's3',
        endpoint_url='https://eodata.dataspace.copernicus.eu',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='default'
    )

    bucket = s3_resource.Bucket(bucket_name)
    objects = list(bucket.objects.filter(Prefix=prefix))

    # for debugging which files will be downloaded
    #print("\nObjects returned from S3:\n")

    #for obj in objects:
    #    print(f"{obj.key}   ({obj.size} bytes)")

    #    if not objects:
    #        print(f"[WARN] No files found for prefix: {prefix}")
    #        return

    total_bytes = sum(obj.size for obj in objects)
    total_mb = total_bytes / (1024 * 1024)

    print(f"\nDownloading {len(objects)} files ({total_mb:.2f} MB) → {target_dir}")

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for obj in objects:

            # Skip directory marker objects
            if obj.key.endswith("/") and obj.size == 0:
                print(f"Skipping directory marker: {obj.key}")
                continue

            relative_path = os.path.relpath(obj.key, prefix)
            local_file_path = os.path.join(target_dir, relative_path)

            futures.append(
                executor.submit(
                    download_single_file,
                    obj.key,
                    bucket_name,
                    local_file_path,
                    access_key,
                    secret_key
                )
            )

        for future in concurrent.futures.as_completed(futures):
            future.result()

    duration = time.time() - start_time

    # ---- stats ----
    duration = max(duration, 1e-6)  # avoid division by zero
    speed_mb_s = total_mb / duration
    speed_mbps = speed_mb_s * 8  # megabits per second

    print(f"\nDownload complete! Files saved in: {os.path.abspath(target_dir)}")
    print("\n--- Parallel Download Statistics ---")
    print(f"Total files:           {len(objects)}")
    print(f"Total data:            {total_mb:.2f} MB")
    print(f"Total time:            {duration:.2f} seconds")
    print(f"Avg speed:             {speed_mb_s:.2f} MB/s")
    print(f"Avg speed:             {speed_mbps:.2f} Mbit/s")
    print(f"Threads used:          {max_threads}")
    print("-----------------------------------\n")


def create_s3_client(access_key, secret_key):
    session = boto3.session.Session()

    return session.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )

def verify_download(
    bucket_name,
    prefix,
    target_dir,
    access_key,
    secret_key,
):
    """
    Verify that every object stored under `prefix` in the S3 bucket
    exists locally and has the correct file size.

    Returns
    -------
    complete : bool
        True if every file matches the remote version.

    failed_files : list of dict
        Each entry contains:
            {
                "key": full S3 key,
                "relative_path": path inside SAFE,
                "reason": "missing" or "size_mismatch",
                "expected_size": int,
                "actual_size": int or None
            }
    """

    s3 = create_s3_client(access_key, secret_key)

    paginator = s3.get_paginator("list_objects_v2")

    failed_files = []

    total_files = 0
    verified_files = 0
    total_bytes = 0
    verified_bytes = 0

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):

        for obj in page.get("Contents", []):

            # Skip S3 directory markers
            if obj["Key"].endswith("/") and obj["Size"] == 0:
                continue

            total_files += 1

            key = obj["Key"]
            remote_size = obj["Size"]

            total_bytes += remote_size

            relative_path = os.path.relpath(key, prefix)
            local_file = os.path.join(target_dir, relative_path)

            # ----------------------------
            # Missing file
            # ----------------------------
            if not os.path.isfile(local_file):

                failed_files.append({
                    "key": key,
                    "relative_path": relative_path,
                    "reason": "missing",
                    "expected_size": remote_size,
                    "actual_size": None,
                })

                continue

            local_size = os.path.getsize(local_file)

            # ----------------------------
            # Wrong file size
            # ----------------------------
            if local_size != remote_size:

                failed_files.append({
                    "key": key,
                    "relative_path": relative_path,
                    "reason": "size_mismatch",
                    "expected_size": remote_size,
                    "actual_size": local_size,
                })

                continue

            verified_files += 1
            verified_bytes += remote_size

    # ----------------------------------------------------
    # Summary
    # ----------------------------------------------------

    print("\n========== Verification ==========")
    print(f"Remote files     : {total_files}")
    print(f"Verified files   : {verified_files}")
    print(f"Failed files     : {len(failed_files)}")
    print(
        f"Verified data    : "
        f"{verified_bytes/(1024**3):.2f} / {total_bytes/(1024**3):.2f} GB"
    )

    if failed_files:

        print("\nProblems found:")

        for f in failed_files:

            if f["reason"] == "missing":
                print(f"  [MISSING] {f['relative_path']}")

            else:
                print(
                    f"  [SIZE]    {f['relative_path']} "
                    f"(local={f['actual_size']} B, "
                    f"expected={f['expected_size']} B)"
                )

        print("==================================\n")

        return False, failed_files

    print("Verification successful.")
    print("==================================\n")

    return True, []


# -----------------------------
# MAIN
# -----------------------------
def main():
    parser = argparse.ArgumentParser()

    # config file
    parser.add_argument(
        "--config",
        default=os.getenv("CONFIG_PATH", "configs/vienna_2020.yaml")
    )

    # acquisition parameters
    parser.add_argument("--orbit_state")
    parser.add_argument("--relative_orbit", type=int)
    parser.add_argument("--instrument_mode")

    # temporal parameters
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")

    # spatial parameters
    parser.add_argument("--bbox")

    # download parameters
    parser.add_argument("--max_threads", type=int)
    parser.add_argument("--base_path")
    parser.add_argument("--max_retries", type=int)

    args = parser.parse_args()

    # -------------------------
    # load config defaults
    # -------------------------
    config = load_config(args.config)

    # ensure structure exists
    config.setdefault("download", {})
    config["download"].setdefault("max_threads", 4)


    config["download"].setdefault("max_retries", 3)

    if args.max_retries is not None:
        config["download"]["max_retries"] = args.max_retries

    # -------------------------
    # CLI overrides
    # -------------------------

    # acquisition
    if args.orbit_state:
        config["orbit_state"] = args.orbit_state

    if args.relative_orbit is not None:
        config["relative_orbit"] = args.relative_orbit

    if args.instrument_mode:
        config["instrument_mode"] = args.instrument_mode

    # temporal
    if args.start_date:
        config["start_date"] = args.start_date

    if args.end_date:
        config["end_date"] = args.end_date

    # spatial
    if args.bbox:
        bbox = [float(x) for x in args.bbox.split()]

        if len(bbox) != 4:
            raise ValueError(
                "--bbox must contain exactly 4 values: WEST SOUTH EAST NORTH"
            )

        config["bbox"] = bbox
    

    # download
    if args.max_threads is not None:
        config["download"]["max_threads"] = args.max_threads

    if args.base_path:
        config["download"]["base_path"] = args.base_path

    # -------------------------
    # credentials
    # -------------------------
    access_key = os.getenv("cdse_S3_KEY")
    secret_key = os.getenv("cdse_S3_SECRET")

    if not access_key or not secret_key:
        raise ValueError("Missing cdse_S3_KEY or cdse_S3_SECRET")

    # -------------------------
    # effective configuration
    # -------------------------
    print("\n=== Effective Parameters ===")
    print(f"orbit_state     : {config['orbit_state']}")
    print(f"relative_orbit  : {config['relative_orbit']}")
    print(f"instrument_mode : {config['instrument_mode']}")
    print(f"start_date      : {config['start_date']}")
    print(f"end_date        : {config['end_date']}")
    print(f"bbox            : {config['bbox']}")
    print(f"base_path       : {config['download']['base_path']}")
    print(f"max_threads     : {config['download']['max_threads']}")
    print("============================\n")

    # -------------------------
    # output path
    # -------------------------
    base_root = os.path.expandvars(config["download"]["base_path"])

    run_folder = build_run_folder(config)

    base_path = os.path.join(base_root, run_folder)

    os.makedirs(base_path, exist_ok=True)

    print(f"Download path: {base_path}\n")

    # -------------------------
    # search scenes
    # -------------------------
    scenes = search_scenes(config)

    print(f"Found {len(scenes)} scenes\n")

    # -------------------------
    # download
    # -------------------------
    MAX_RETRIES = config["download"]["max_retries"]
    MAX_THREADS = config["download"]["max_threads"]

    for i, s in enumerate(scenes, 1):

        product_name = s["product_name"]

        print(f"[{i}/{len(scenes)}] DOWNLOAD {product_name}")

        bucket, prefix = get_s3_path(product_name)

        if not bucket:
            print("Could not determine S3 path. Skipping.")
            continue

        target_dir = os.path.join(base_path, product_name)

        # Skip if already completely downloaded
        if safe_exists(base_path, product_name):

            ok, _ = verify_download(
                bucket,
                prefix,
                target_dir,
                access_key,
                secret_key,
            )

            if ok:
                print("SAFE already complete. Skipping.")
                continue

            print("Incomplete SAFE detected. Continuing download...")

        else:
            print("SAFE not found. Starting download...")


        # Retry loop
        for attempt in range(1, MAX_RETRIES + 1):

            print(f"\nDownload attempt {attempt}/{MAX_RETRIES}")

            download_product_from_s3_parallel(
                bucket,
                prefix,
                target_dir,
                access_key,
                secret_key,
                max_threads=MAX_THREADS
            )

            ok, failed_files = verify_download(
                bucket,
                prefix,
                target_dir,
                access_key,
                secret_key,
            )

            if ok:
                print("✓ Download verified successfully.")
                break

            print(f"Verification failed ({len(failed_files)} missing/corrupted files).")


            # Remove corrupted files so they will be downloaded again
            for f in failed_files:

                if f["reason"] == "size_mismatch":

                    local_file = os.path.join(
                        target_dir,
                        f["relative_path"]
                    )

                    if os.path.exists(local_file):
                        os.remove(local_file)

            if attempt < MAX_RETRIES:
                print("Retrying...")    

        else:
            raise RuntimeError(
                f"Could not completely download {product_name} after "
                f"{MAX_RETRIES} attempts."
            )

    print("\nAll done.\n")


if __name__ == "__main__":
    main()