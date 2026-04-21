import os
import argparse
import requests
import yaml
import boto3
import time
import concurrent.futures
from utils import load_config
from dotenv import load_dotenv


load_dotenv("cdse.env")

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

    if not objects:
        print(f"[WARN] No files found for prefix: {prefix}")
        return

    total_bytes = sum(obj.size for obj in objects)
    total_mb = total_bytes / (1024 * 1024)

    print(f"\nDownloading {len(objects)} files ({total_mb:.2f} MB) → {target_dir}")

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for obj in objects:
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

        concurrent.futures.wait(futures)

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


# -----------------------------
# MAIN
# -----------------------------
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        default=os.getenv("CONFIG_PATH", "configs/vienna_2020.yaml")
    )

    # overrides for Argo / CLI
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--bbox", nargs=4, type=float)
    parser.add_argument("--relative_orbit", type=int)
    parser.add_argument("--max_threads", type=int)

    args = parser.parse_args()

    config = load_config(args.config)

    # ensure defaults exist
    config.setdefault("download", {})
    config["download"].setdefault("max_threads", 4)

    # override config if provided
    if args.start_date:
        config["start_date"] = args.start_date

    if args.end_date:
        config["end_date"] = args.end_date

    if args.bbox:
        config["bbox"] = args.bbox

    if args.relative_orbit:
        config["relative_orbit"] = args.relative_orbit

    if args.max_threads:
        config.setdefault("download", {})
        config["download"]["max_threads"] = args.max_threads


    access_key = os.getenv("cdse_S3_KEY")
    secret_key = os.getenv("cdse_S3_SECRET")

    if not access_key or not secret_key:
        raise ValueError("Missing ACCESS_KEY or SECRET_KEY")

    base_root = os.path.expandvars(config["download"]["base_path"])
    run_folder = build_run_folder(config)

    base_path = os.path.join(base_root, run_folder)
    os.makedirs(base_path, exist_ok=True)

    print("\n=== Effective Parameters ===")
    for key in ["orbit_state", "relative_orbit", "start_date", "end_date", "bbox"]:
        print(f"{key:16}: {config.get(key)}")

    print(f"{'max_threads':16}: {config['download'].get('max_threads')}")
    print("============================\n")

    print(f"Download path: {base_path}\n")

    scenes = search_scenes(config)

    print(f"Found {len(scenes)} scenes\n")

    for i, s in enumerate(scenes, 1):
        product_name = s["product_name"]

        if safe_exists(base_path, product_name):
            print(f"[{i}/{len(scenes)}] SKIP {product_name}")
            continue

        print(f"[{i}/{len(scenes)}] DOWNLOAD {product_name}")

        bucket, prefix = get_s3_path(product_name)

        if not bucket:
            continue

        target_dir = os.path.join(base_path, product_name)

        download_product_from_s3_parallel(
            bucket,
            prefix,
            target_dir,
            access_key,
            secret_key,
            max_threads=config["download"].get("max_threads", 4)
        )

    print("\nAll done.\n")


if __name__ == "__main__":
    main()