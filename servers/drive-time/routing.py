"""Routing engine abstraction for OSRM and OpenRouteService.

OSRM:
  - Public demo server (router.project-osrm.org) is fine for prototyping but
    rate-limited. For production workloads, deploy a self-hosted OSRM instance
    and set OSRM_BASE_URL accordingly.

ORS (OpenRouteService):
  - Requires an API key from https://openrouteservice.org/
  - Set ORS_API_KEY environment variable.
"""

from shared.utils.http_client import resilient_request, osrm_rate_limiter

_OSRM_PUBLIC_HOST = "router.project-osrm.org"


class OSRMRouter:
    """Client for the OSRM routing engine (route + table endpoints)."""

    def __init__(self, base_url: str = "http://router.project-osrm.org"):
        self.base_url = base_url.rstrip("/")
        # Enable rate limiting only for the public demo server
        self._is_public = _OSRM_PUBLIC_HOST in self.base_url

    async def _get(self, url: str, *, params: dict | None = None, timeout: float = 30.0):
        """GET with optional OSRM public-server rate limiting."""
        if self._is_public:
            async with osrm_rate_limiter:
                return await resilient_request("GET", url, params=params, timeout=timeout)
        return await resilient_request("GET", url, params=params, timeout=timeout)

    async def route(self, origin: tuple[float, float], dest: tuple[float, float]) -> dict:
        """Compute a single route between two points.

        Args:
            origin: (lon, lat) tuple
            dest: (lon, lat) tuple

        Returns:
            dict with duration_seconds and distance_meters
        """
        coords = f"{origin[0]},{origin[1]};{dest[0]},{dest[1]}"
        url = f"{self.base_url}/route/v1/car/{coords}"
        resp = await self._get(url, params={"overview": "false"}, timeout=30.0)
        data = resp.json()
        if data.get("code") != "Ok":
            raise RuntimeError(f"OSRM route error: {data.get('code')} — {data.get('message', '')}")
        route_data = data["routes"][0]
        return {
            "duration_seconds": route_data["duration"],
            "distance_meters": route_data["distance"],
        }

    async def table(
        self,
        coords: list[tuple[float, float]],
        sources: list[int] | None = None,
        destinations: list[int] | None = None,
    ) -> dict:
        """Compute an NxM duration/distance matrix.

        Args:
            coords: list of (lon, lat) tuples — all origins and destinations combined
            sources: indices into coords for origin rows (None = all)
            destinations: indices into coords for destination columns (None = all)

        Returns:
            Raw OSRM table response with durations and distances arrays.
        """
        coords_str = ";".join(f"{c[0]},{c[1]}" for c in coords)
        url = f"{self.base_url}/table/v1/car/{coords_str}"
        params: dict = {"annotations": "duration,distance"}
        if sources is not None:
            params["sources"] = ";".join(str(s) for s in sources)
        if destinations is not None:
            params["destinations"] = ";".join(str(d) for d in destinations)

        resp = await self._get(url, params=params, timeout=120.0)
        data = resp.json()
        if data.get("code") != "Ok":
            raise RuntimeError(f"OSRM table error: {data.get('code')} — {data.get('message', '')}")
        return data


class ORSRouter:
    """Client for the OpenRouteService API (isochrones)."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.openrouteservice.org"

    async def isochrone(self, lon: float, lat: float, ranges: list[int]) -> dict:
        """Generate isochrone polygons.

        Args:
            lon: Longitude of center point
            lat: Latitude of center point
            ranges: List of time thresholds in seconds (e.g., [900, 1800, 3600])

        Returns:
            GeoJSON FeatureCollection with isochrone polygons.
        """
        url = f"{self.base_url}/v2/isochrones/driving-car"
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }
        body = {
            "locations": [[lon, lat]],
            "range": ranges,
            "range_type": "time",
        }
        resp = await resilient_request("POST", url, json=body, headers=headers, timeout=30.0)
        return resp.json()
