import os
import argparse
import requests
import boto3

from utils import load_config


# -----------------------------
# SAFE EXISTENCE CHECK (S3)
# -----------------------------
def safe_exists_s3(bucket_name, base_prefix, product_name, access_key, secret_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url="https://obs.eu-de.otc.t-systems.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="eu-de",
    )

    key = f"{base_prefix.rstrip('/')}/{product_name}/manifest.safe"

    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3_client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ["404", "NoSuchKey", "NotFound"]:
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

    base_url = "https://stac.dataspace.copernicus.eu/v1/search"
    url = base_url
    items = []

    while url:
        if url == base_url:
            r = requests.post(url, json=payload)
        else:
            r = requests.post(url)

        r.raise_for_status()
        data = r.json()

        items.extend(data.get("features", []))

        next_link = next(
            (l for l in data.get("links", []) if l["rel"] == "next"),
            None
        )
        url = next_link["href"] if next_link else None

    # filter VV
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
# COPY S3 → S3
# -----------------------------
def copy_product_s3(src_bucket, src_prefix, dst_bucket, dst_prefix, access_key, secret_key):
    s3_src = boto3.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )

    s3_dst = boto3.client(
        "s3",
        endpoint_url="https://obs.eu-de.otc.t-systems.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="eu-de",
    )

    paginator = s3_src.get_paginator("list_objects_v2")

    found = False

    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            found = True

            key = obj["Key"]
            rel = os.path.relpath(key, src_prefix)
            dst_key = f"{dst_prefix}/{rel}"

            print(f"Copying {key} → {dst_key}")

            try:
                response = s3_src.get_object(Bucket=src_bucket, Key=key)
                s3_dst.upload_fileobj(response["Body"], dst_bucket, dst_key)
            except Exception as e:
                print(f"[ERROR] Failed copying {key}: {e}")
                raise

    if not found:
        print(f"[WARNING] No files found for prefix: {src_prefix}")


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

        print(f"[{i}/{len(scenes)}] COPY {product}")

        bucket, prefix = get_s3_path(product)
        if not bucket:
            print(f"[WARNING] No S3 path found for {product}")
            continue

        copy_product_s3(
            bucket,
            prefix,
            TARGET_BUCKET,
            f"{TARGET_PREFIX}/{product}",
            access_key,
            secret_key
        )

    print("\nDone.")


if __name__ == "__main__":
    main()