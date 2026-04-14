FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
        cdo \
        bzip2 \
        curl \
        ca-certificates \
        git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
 && pip install \
        xarray \
        dask \
        cfgrib \
        zarr \
        s3fs \
        numcodecs \
        numpy \
        requests \
        netCDF4

WORKDIR /workspace
