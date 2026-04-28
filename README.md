# SLC_downloader
This repo allows you to download any SLC Sentinel-1 files from CDSE

There are two versions.

1. Work on local system with normal folder and file structure:

    - use download_slc.py

    This script:

    Searches Sentinel-1 SLC scenes via STAC
    Resolves their location in Copernicus Data Space (S3)
    Downloads all files of each .SAFE product
    Stores them locally in a structured folder
    Skips already downloaded products

    👉 Uses parallel downloads
    👉 Keeps full .SAFE structure
    👉 Works with mounted Docker volume



    go to your project directory where data folder is located:

    e.g.: 

    cd ~/projects/ADUCAT/data

    🔐 Required Environment Variables

    You only need Copernicus Data Space credentials:

    cdse_S3_KEY
    cdse_S3_SECRET

    These are used to access:

    https://eodata.dataspace.copernicus.eu

    ⚙️ Configuration
    Example config (configs/vienna_2020.yaml):

    orbit_state: ascending
    relative_orbit: 73
    instrument_mode: IW

    start_date: "2020-06-01"
    end_date: "2020-07-01"

    bbox:
    - 16.1840505
    - 48.119439
    - 16.578036
    - 48.32185013

    download:
    base_path: "/data"
    max_threads: 6

    ▶️ Run Locally (Docker)
    ✅ Step 1 — prepare local folder
    mkdir -p data

    ✅ Step 2 — export credentials
    export cdse_S3_KEY=...
    export cdse_S3_SECRET=...

    ✅ Step 3 — run container
    docker run --rm \
    -v $(pwd)/data:/data \
    -e cdse_S3_KEY \
    -e cdse_S3_SECRET \
    ghcr.io/lukas-scharf-plus/slc-downloader:0.1.1 \
    python src/download_slc.py --config configs/vienna_2020.yaml

    📂 Output Folder Structure

    After running:

    data/
    └── S1_073_ASC/
        ├── S1A_IW_SLC__1SDV_20200628T...SAFE/
        │   ├── manifest.safe
        │   ├── annotation/
        │   ├── measurement/
        │   └── ...
        ├── S1B_IW_SLC__1SDV_20200622T...SAFE/
        └── ...




2. Work in cloud, copy needed S3 SLC files from one S3 bucket to your target S3 bucket

    - use download_slc_stream.py

    This script:

    Searches Sentinel-1 SLC scenes via STAC
    Resolves their source location in Copernicus Data Space (S3)
    Copies each product directly from CDSE S3 → your workspace S3
    Skips already existing products

    👉 No local storage required
    👉 No intermediate files
    👉 Fully streaming S3 → S3



    🔐 Required Environment Variables

    You must provide two sets of credentials:

    1. Copernicus Data Space (source)

    cdse_S3_KEY
    cdse_S3_SECRET

    Used to read from:

    https://eodata.dataspace.copernicus.eu

    2. Workspace S3 (destination)

    WORKSPACE_URL           # e.g. https://obs.eu-de.otc.t-systems.com
    WORKSPACE_REGION        # usually eu-de
    WORKSPACE_ACCESS_KEY
    WORKSPACE_SECRET_KEY
    WORKSPACE_BUCKET        # e.g. aducat


    ⚙️ Configuration

    The script expects a YAML config like:

    orbit_state: ascending
    relative_orbit: 73
    instrument_mode: IW

    start_date: "2020-06-01"
    end_date: "2020-07-01"

    bbox:
    - 16.1840505
    - 48.119439
    - 16.578036
    - 48.32185013


    ▶️ Run Locally (Docker)
    ✅ Option 1 — using exported environment variables
    export cdse_S3_KEY=...
    export cdse_S3_SECRET=...

    export WORKSPACE_URL=https://obs.eu-de.otc.t-systems.com
    export WORKSPACE_REGION=eu-de
    export WORKSPACE_ACCESS_KEY=...
    export WORKSPACE_SECRET_KEY=...
    export WORKSPACE_BUCKET=aducat


    Then run:

    docker run --rm \
    -e cdse_S3_KEY \
    -e cdse_S3_SECRET \
    -e WORKSPACE_URL \
    -e WORKSPACE_REGION \
    -e WORKSPACE_ACCESS_KEY \
    -e WORKSPACE_SECRET_KEY \
    -e WORKSPACE_BUCKET \
    ghcr.io/lukas-scharf-plus/slc-downloader:0.1.1 \
    python src/download_slc_stream.py --config configs/vienna_2020.yaml


    📦 Output Structure (in your S3 bucket)
    s3://<WORKSPACE_BUCKET>/
    └── WP5_Infrastructure_and_Underground_Safety/
        └── data/
            └── S1_073_ASC/
                ├── S1A_...SAFE/
                ├── S1B_...SAFE/
                └── ...