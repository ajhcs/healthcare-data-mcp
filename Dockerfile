FROM python:3.12-slim

WORKDIR /app

# Install system deps for geopandas (GEOS, GDAL, PROJ)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    libgdal-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir \
    "mcp[cli]>=1.0.0" \
    httpx \
    pandas \
    pydantic \
    geopandas \
    networkx \
    shapely \
    duckdb \
    beautifulsoup4 \
    lxml \
    rapidfuzz \
    pyarrow \
    polars

# Copy source code
COPY shared/ shared/
COPY servers/ servers/

# Default environment
ENV MCP_TRANSPORT=streamable-http
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
