"""Pydantic models for drive-time and accessibility data."""

from pydantic import BaseModel, Field


class Location(BaseModel):
    """A geographic point with optional identifier."""

    lat: float = Field(description="Latitude")
    lon: float = Field(description="Longitude")
    id: str = Field(default="", description="Optional identifier for the location")


class RouteResult(BaseModel):
    """Result of a single point-to-point route calculation."""

    duration_seconds: float = Field(description="Travel time in seconds")
    duration_minutes: float = Field(description="Travel time in minutes")
    distance_meters: float = Field(description="Distance in meters")
    distance_miles: float = Field(description="Distance in miles")


class MatrixEntry(BaseModel):
    """A single cell in a drive-time matrix."""

    origin_id: str
    destination_id: str
    duration_minutes: float | None = None
    distance_miles: float | None = None


class NearbyFacility(BaseModel):
    """A healthcare facility within driving distance of a point."""

    ccn: str = Field(description="CMS Certification Number")
    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    drive_time_minutes: float = 0.0
    distance_miles: float = 0.0


class DemandPoint(BaseModel):
    """A demand location for accessibility scoring (e.g., ZCTA centroid)."""

    lat: float
    lon: float
    population: float = Field(description="Population or demand weight")
    id: str = ""


class SupplyPoint(BaseModel):
    """A supply location for accessibility scoring (e.g., hospital)."""

    lat: float
    lon: float
    capacity: float = Field(description="Capacity (e.g., bed count)")
    id: str = ""


class AccessibilityResult(BaseModel):
    """Accessibility score for a single demand point."""

    demand_id: str
    lat: float
    lon: float
    population: float
    accessibility_score: float = Field(description="2SFCA accessibility index")


class AccessibilitySummary(BaseModel):
    """Summary statistics for an accessibility analysis."""

    num_demand_points: int
    num_supply_points: int
    catchment_minutes: int
    mean_score: float
    median_score: float
    min_score: float
    max_score: float
    std_score: float
    points_with_zero_access: int
    results: list[AccessibilityResult]
