FROM python:3.12-slim

WORKDIR /app

# Install system deps for geopandas (GEOS, GDAL, PROJ)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    libgdal-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source code and install the package so Docker uses the same dependency
# metadata and package selection as local installs.
COPY pyproject.toml README.md ./
COPY shared/ shared/
COPY servers/ servers/
RUN pip install --no-cache-dir .

# Default environment
ENV MCP_TRANSPORT=streamable-http
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
