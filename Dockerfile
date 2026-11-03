# Docker image for the Sentinel-1 SLC and orbit downloaders.
# The Python environment is built from environment.yml, so the container
# uses exactly the same dependencies as the local conda env.
#
# Build: docker build -t slc-downloader .
# Run:   docker run --rm slc-downloader python src/download_slc.py --help

FROM mambaorg/micromamba:2.3.0

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml
RUN micromamba install --yes --name base --file /tmp/environment.yml \
    && micromamba clean --all --yes

WORKDIR /app
COPY --chown=$MAMBA_USER:$MAMBA_USER src/ ./src/
COPY --chown=$MAMBA_USER:$MAMBA_USER configs/ ./configs/
# The tests are small and offline; GitHub Actions runs them inside the image.
COPY --chown=$MAMBA_USER:$MAMBA_USER tests/ ./tests/

# Print log lines immediately instead of buffering them.
ENV PYTHONUNBUFFERED=1

# The micromamba base image's entrypoint activates the environment,
# so "python" below is the environment's Python.
CMD ["python", "src/download_slc.py", "--help"]
