import os
import argparse
import requests
import boto3
import time
import concurrent.futures
import shutil

from utils import load_config


# -----------------------------
# SAFE EXISTENCE CHECK (S3)
# -----------------------------
def safe_exists_s3(bucket_name, base_prefix, product_name, access_key, secret_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )

    key = f"{base_prefix.rstrip('/')}/{product_name}/manifest.safe"

    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise


# -----------------------------
# RUN FOLDER NAME
# -----------------------------
def build_run_folder(config):
    orbit = config["orbit_state"][:3].upper()
    rel_orbit = str(config["relative_orbit"]).zfill(3)
    return f"S1_{rel_orbit}_{orbit}"


# -----------------------------
# STAC SEARCH
# -----------------------------
def search_scenes(config):
    payload = {
        "collections": ["sentinel-1-slc"],
        "bbox": config["bbox"],
        "datetime": f"{config['start_date']}T00:00:00Z/{config['end_date']}T00:00:00Z",
        "limit": 500,
        "query": {
            "sat:orbit_state": {"eq": config["orbit_state"].lower()},
            "sat:relative_orbit": {"eq": config["relative_orbit"]},
            "sar:instrument_mode": {"eq": config["instrument_mode"]}
        }
    }

    url = "https://stac.dataspace.copernicus.eu/v1/search"
    items = []

    while url:
        r = requests.post(url, json=payload if url.endswith("/search") else None)
        r.raise_for_status()
        data = r.json()

        items.extend(data.get("features", []))

        next_link = next((l for l in data.get("links", []) if l["rel"] == "next"), None)
        url = next_link["href"] if next_link else None

    items = [
        it for it in items
        if "VV" in it["properties"].get("sar:polarizations", [])
    ]

    scenes = []
    for it in items:
        product_name = it["id"]
        if not product_name.endswith(".SAFE"):
            product_name += ".SAFE"

        scenes.append({"product_name": product_name})

    return scenes


# -----------------------------
# ODATA → S3 PATH
# -----------------------------
def get_s3_path(product_name):
    url = (
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        f"?$filter=Name eq '{product_name}'"
    )

    r = requests.get(url)
    r.raise_for_status()

    data = r.json().get("value", [])
    if not data:
        return None, None

    s3_path = data[0]["S3Path"]
    parts = s3_path.lstrip("/").split("/", 1)

    return parts[0], parts[1]


# -----------------------------
# DOWNLOAD (parallel)
# -----------------------------
def download_single_file(s3_key, bucket_name, local_path, access_key, secret_key):
    if os.path.exists(local_path):
        return

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )

    s3.download_file(bucket_name, s3_key, local_path)


def download_product(bucket, prefix, target_dir, access_key, secret_key, max_threads):
    s3 = boto3.resource(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )

    objects = list(s3.Bucket(bucket).objects.filter(Prefix=prefix))
    if not objects:
        return

    print(f"Downloading {len(objects)} files → {target_dir}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as exe:
        futures = []
        for obj in objects:
            rel = os.path.relpath(obj.key, prefix)
            local = os.path.join(target_dir, rel)

            futures.append(
                exe.submit(download_single_file, obj.key, bucket, local, access_key, secret_key)
            )

        concurrent.futures.wait(futures)


# -----------------------------
# UPLOAD
# -----------------------------
def upload_directory(local_dir, prefix):
    bucket = os.getenv("WORKSPACE_BUCKET")
    access_key = os.getenv("cdse_S3_KEY")
    secret_key = os.getenv("cdse_S3_SECRET")

    s3 = boto3.client(
        "s3",
        endpoint_url="https://workspace.aducat.hub-otc-sc.eox.at",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )

    for root, _, files in os.walk(local_dir):
        for f in files:
            local_path = os.path.join(root, f)
            rel = os.path.relpath(local_path, local_dir)

            key = f"{prefix}/{os.path.basename(local_dir)}/{rel}"

            s3.upload_file(local_path, bucket, key)

# -----------------------------
# MAIN
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/vienna_2020.yaml")
    args = parser.parse_args()

    config = load_config(args.config)

    access_key = os.getenv("cdse_S3_KEY")
    secret_key = os.getenv("cdse_S3_SECRET")
    TARGET_BUCKET = os.getenv("WORKSPACE_BUCKET")

    if not all([access_key, secret_key, TARGET_BUCKET]):
        raise ValueError("Missing credentials or WORKSPACE_BUCKET")

    run_folder = build_run_folder(config)
    TARGET_PREFIX = f"WP5_Infrastructure_and_Underground_Safety/data/{run_folder}"

    print(f"Target: s3://{TARGET_BUCKET}/{TARGET_PREFIX}")

    scenes = search_scenes(config)

    for i, s in enumerate(scenes, 1):
        product = s["product_name"]

        if safe_exists_s3(TARGET_BUCKET, TARGET_PREFIX, product, access_key, secret_key):
            print(f"[{i}/{len(scenes)}] SKIP {product}")
            continue

        print(f"[{i}/{len(scenes)}] DOWNLOAD {product}")

        bucket, prefix = get_s3_path(product)
        if not bucket:
            continue

        local_dir = os.path.join("/data", run_folder, product)

        download_product(
            bucket,
            prefix,
            local_dir,
            access_key,
            secret_key,
            config["download"].get("max_threads", 4),
        )

        upload_directory(local_dir, TARGET_BUCKET, TARGET_PREFIX)

        # cleanup to save disk
        shutil.rmtree(local_dir, ignore_errors=True)

    print("\nDone.")


if __name__ == "__main__":
    main()