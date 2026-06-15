FROM python:3.12-slim

ARG VERSION=0.4.0
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="Healthcare Data MCP" \
      org.opencontainers.image.description="Public healthcare data MCP servers for local agents and controlled gateways" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$VCS_REF" \
      org.opencontainers.image.created="$BUILD_DATE" \
      org.opencontainers.image.source="https://github.com/ajhcs/healthcare-data-mcp" \
      org.opencontainers.image.licenses="MIT"

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
