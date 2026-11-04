# SLC_downloader

Downloads Sentinel-1 **SLC products** (complete `.SAFE` folders) and the matching
**orbit files** (`.EOF`) from the [Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu).
Works for all Sentinel-1 satellites: **S1A, S1B, S1C and S1D**.

> Work in progress. Functionality and correctness are not guaranteed; use at your own risk.

Two command-line tools:

| Script | What it does |
| --- | --- |
| `src/download_slc.py` | Searches SLC products by track, date range and area and downloads them, with resume, retries and checksum verification. |
| `src/download_orbits.py` | Finds the `.SAFE` folders you already have and downloads an orbit file for each one: precise (POEORB) if available, otherwise restituted (RESORB). |

Every parameter can be set in a **YAML config**, on the **command line**, or both.
Command-line values override config values.

---

## Setup

### 1. CDSE S3 credentials

Both tools read from the CDSE S3 storage and need S3 keys
(create them at <https://eodata-s3keysmanager.dataspace.copernicus.eu>):

| Variable | Meaning |
| --- | --- |
| `cdse_S3_KEY` | CDSE S3 access key |
| `cdse_S3_SECRET` | CDSE S3 secret key |

Either export them in your shell or put them in a `.env` file in the repository root
(the file is git-ignored and never copied into the Docker image):

```
cdse_S3_KEY=...
cdse_S3_SECRET=...
```

### 2a. Local: conda environment

```bash
conda env create -f environment.yml        # first time
conda env update -n slc-downloader -f environment.yml --prune   # after changes
conda activate slc-downloader
```

### 2b. Docker

Use the published image (tags are listed under the repository's *Packages* on GitHub):

```bash
docker pull ghcr.io/lukas-scharf-plus/slc_downloader:latest
```

Or build it yourself from this repository:

```bash
docker build -t ghcr.io/lukas-scharf-plus/slc_downloader:latest .
```

Publishing is automatic: GitHub Actions (`.github/workflows/docker.yml`) builds
the image on every push to `main`, runs the tests in it, and publishes it with the
tags `latest` and `sha-<commit>`. To publish a version number, push a git tag:

```bash
git tag v0.3.0
git push origin v0.3.0      # publishes slc_downloader:0.3.0 and :0.3
```

The image name `slc_downloader` comes from the repository name. Versions up to
0.2.1 were pushed by hand as `ghcr.io/lukas-scharf-plus/slc-downloader` (hyphen);
they stay available there, but new images only go to `slc_downloader`.

---

## Usage

### Download SLC products

With a config file (see [configs/vienna.yaml](configs/vienna.yaml)):

```bash
python src/download_slc.py --config configs/vienna.yaml
```

Without a config file, everything on the command line:

```bash
python src/download_slc.py \
    --orbit_state ascending \
    --relative_orbit 146 \
    --instrument_mode IW \
    --start_date 2021-01-01 \
    --end_date 2021-01-31 \
    --bbox "16.18 48.11 16.58 48.33" \
    --base_path ~/projects/ADUCAT/data/slc
```

Config plus overrides (e.g. a different date range):

```bash
python src/download_slc.py --config configs/vienna.yaml --start_date 2023-01-01 --end_date 2023-12-31
```

Only list what would be downloaded:

```bash
python src/download_slc.py --config configs/vienna.yaml --dry_run
```

Only some satellites, e.g. S1A and S1C (default: all available Sentinel-1 satellites):

```bash
python src/download_slc.py --config configs/vienna.yaml --platforms "S1A S1C"
```

This is useful for InSAR stacks that should only contain one satellite, for example
when interferograms between two satellites show phase jumps at burst edges
(timing differences between the satellites that are not corrected without ESD).

| Option | Config key | Default | Meaning |
| --- | --- | --- | --- |
| `--orbit_state` | `orbit_state` | required | `ascending` or `descending` |
| `--relative_orbit` | `relative_orbit` | required | track number, 1–175 |
| `--instrument_mode` | `instrument_mode` | required | `IW`, `EW`, `SM` or `WV` |
| `--polarization` | `polarization` | `VV` | products must contain this polarization |
| `--platforms` | `platforms` | all | satellites to download: any of `S1A S1B S1C S1D` (also `A`, `c`, `sentinel-1a`; separated by spaces or commas), or `all` |
| `--start_date` | `start_date` | required | first day, `YYYY-MM-DD` |
| `--end_date` | `end_date` | required | last day, **inclusive** |
| `--bbox` | `bbox` | required | `"WEST SOUTH EAST NORTH"` in degrees |
| `--base_path` | `download.base_path` | required | root output folder |
| `--max_threads` | `download.max_threads` | 4 | files downloaded in parallel |
| `--threads_per_file` | `download.threads_per_file` | 1 | parallel connections per large file |
| `--max_connections` | `download.max_connections` | 4 | upper limit for `max_threads × threads_per_file` |
| `--max_retries` | `download.max_retries` | 3 | download attempts per product |
| `--dry_run` | – | off | search only, no download |

**CDSE connection limit:** general CDSE users may open at most **4 concurrent S3 connections**
(20 MB/s each, 2000 requests/minute, see the [CDSE quotas](https://documentation.dataspace.copernicus.eu/Quotas.html)).
Extra connections are rejected with `429 Too Many Requests`. The script therefore refuses to start if
`max_threads × threads_per_file` is larger than `max_connections` (default 4). The default of
4 files × 1 connection uses the full quota. The limit applies per account, so do not run two downloads
(e.g. two Argo workflows) with the same credentials at the same time. Only raise `max_connections` if
your account really has a higher quota.

Output layout:

```
<base_path>/
└── S1_146_ASC/
    ├── S1A_IW_SLC__1SDV_20210111T165103_...SAFE/
    │   ├── manifest.safe
    │   ├── annotation/
    │   ├── measurement/
    │   └── ...
    └── S1B_IW_SLC__1SDV_20210117T165023_...SAFE/
```

### Download orbit files

```bash
python src/download_orbits.py \
    --safe_dir ~/projects/ADUCAT/data/slc \
    --orbit_dir ~/projects/ADUCAT/data/orbits
```

or with the `orbits:` section of a config file:

```bash
python src/download_orbits.py --config configs/vienna.yaml
```

| Option | Config key | Meaning |
| --- | --- | --- |
| `--safe_dir` | `orbits.safe_dir` | folder searched (recursively) for `.SAFE` folders |
| `--orbit_dir` | `orbits.orbit_dir` | folder where the `.EOF` files are stored |

Run it again later: scenes that only have a RESORB file get the precise orbit as soon as
CDSE publishes it (about 20 days after acquisition), and the replaced RESORB file is deleted.

### Running in Docker

Mount your data folder at `/data`, pass the credentials, and run the container with your own
user ID so the downloaded files belong to you and not to the container user:

```bash
docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v ~/projects/ADUCAT/data:/data \
    -e cdse_S3_KEY -e cdse_S3_SECRET \
    ghcr.io/lukas-scharf-plus/slc_downloader:latest \
    python src/download_slc.py \
        --orbit_state ascending --relative_orbit 146 --instrument_mode IW \
        --start_date 2021-01-01 --end_date 2021-01-31 \
        --bbox "16.18 48.11 16.58 48.33" \
        --base_path /data/slc

docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v ~/projects/ADUCAT/data:/data \
    -e cdse_S3_KEY -e cdse_S3_SECRET \
    ghcr.io/lukas-scharf-plus/slc_downloader:latest \
    python src/download_orbits.py --safe_dir /data/slc --orbit_dir /data/orbits
```

Instead of `-e cdse_S3_KEY -e cdse_S3_SECRET` you can use `--env-file .env`.
To use your own config, mount it, e.g. `-v $PWD/my.yaml:/app/configs/my.yaml`, then
pass `--config configs/my.yaml`.

### Running on Argo Workflows

[argo/download_slc_and_orbits.yaml](argo/download_slc_and_orbits.yaml) runs both tools as a DAG:
`download-slc` first, then `download-orbits`. Both steps share one volume mounted at `/data`,
so the orbit step finds the SAFE folders of the first step. Each step uploads its folder
(`/data/slc` and `/data/Orbit_files`) as an output artifact to `<s3_output_prefix>/slc` and
`<s3_output_prefix>/Orbit_files` in the artifact bucket.

```bash
argo submit argo/download_slc_and_orbits.yaml \
    -p start_date=2021-01-01 -p end_date=2021-01-31

# only some satellites (parameter "platforms", default "all")
argo submit argo/download_slc_and_orbits.yaml -p platforms="S1A"
```

Things the workflow relies on:

- a Kubernetes secret `cdse` with the keys `S3_KEY` and `S3_SECRET` (both steps need it);
- `command` starts with `/usr/local/bin/_entrypoint.sh`, because in Kubernetes `command`
  replaces the image's entrypoint, and that script activates the Python environment;
- `securityContext` with `fsGroup: 57439`, because the image runs as the non-root user
  `mambauser` and the volume must be writable for it;
- a volume big enough for the period (about 8 GB per IW SLC; the default is 50Gi).

Cluster-specific settings (`serviceAccountName`, node affinity, tolerations, storage class)
are those of the ADUCAT cluster; adjust them for another cluster.

### Exit codes

Both scripts exit with `0` when everything is complete, `1` when at least one
product or orbit failed (the summary at the end lists which ones), and `2` for invalid
parameters or missing credentials. This makes failures visible in scripts and
batch jobs.

---

## How it works

**SLC download (`download_slc.py`)**

1. **Search**: one query to the CDSE STAC catalogue (`stac.dataspace.copernicus.eu`)
   with track, orbit direction, mode, area and dates. All result pages are followed.
   Each STAC item already contains the S3 location of its files.
2. **Check**: for each product, all files of the SAFE folder are listed on S3. Local files
   with the correct size count as done. A product where everything is done is skipped.
3. **Download**: `manifest.safe` is downloaded first; it contains an MD5 checksum for
   most files. Then all missing files are downloaded in parallel from
   `eodata.dataspace.copernicus.eu`. boto3 writes each file under a temporary name and only
   renames it when it is finished, so an interrupted run never leaves a half-written file
   under its real name.
4. **Verify**: each newly downloaded file is checked for size and, if listed in the
   manifest, MD5. Bad files are deleted and downloaded again (up to `max_retries` rounds).
   Files from earlier runs are only checked by size, so re-runs over a large archive stay fast.
5. **Continue**: a product that still fails is reported and the script moves on to the
   next one.

Temporary network problems (timeouts, HTTP 429/5xx) are retried automatically with
backoff, both for the catalogue and for S3.

**Orbit download (`download_orbits.py`)**

1. Read satellite (S1A–S1D) and start/stop time from each `.SAFE` folder name.
2. Check `orbit_dir` for an orbit file of the same satellite that covers the
   acquisition plus 60 s on each side.
3. If there is no precise orbit locally, ask the CDSE OData catalogue
   (`catalogue.dataspace.copernicus.eu`) for the newest POEORB that covers the scene,
   then for a RESORB if no POEORB exists, and download it from CDSE S3.

---

## Project structure

```
SLC_downloader/
├── argo/
│   └── download_slc_and_orbits.yaml  Argo workflow: SLC step, then orbit step, both uploaded to S3
├── configs/
│   └── vienna.yaml        example config (search, download and orbit settings)
├── src/
│   ├── download_slc.py    CLI: search and download SLC products
│   ├── download_orbits.py CLI: download orbit files for local SAFE folders
│   ├── cdse.py            CDSE endpoints, credentials, HTTP session with retries, S3 client
│   ├── config.py          read YAML, merge with command-line values, validate
│   ├── stac_search.py     STAC search with pagination, S3 location of each product
│   ├── safe_download.py   download a SAFE folder, verify size and MD5, retry
│   └── orbits.py          parse scene/orbit names, find and download orbit files
├── tests/                 unit tests (offline, CDSE is replaced by fakes)
├── .github/workflows/
│   └── docker.yml         builds, tests and publishes the image to ghcr.io
├── Dockerfile             image built from environment.yml
├── environment.yml        conda environment "slc-downloader"
└── README.md
```

---

## Tests

The tests use only the Python standard library (`unittest`) and do not touch the network:

```bash
conda activate slc-downloader
python -m unittest discover -s tests -t .
```
