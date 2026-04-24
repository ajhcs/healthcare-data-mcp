"""Pydantic response models for community-health tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SourceMetadata(BaseModel):
    name: str
    dataset_title: str = ""
    dataset_id: str = ""
    geography_type: str | None = None
    release: str = ""
    source_url: str = ""
    landing_page: str = ""
    modified: str = ""
    record_count: int | None = None
    domain: str = "data.cdc.gov"
    interpretation: str


class PlacesMeasure(BaseModel):
    year: int | str | None = None
    geography_type: str
    location_id: str
    location_name: str
    state_abbr: str | None = None
    state_name: str | None = None
    category: str = ""
    category_id: str = ""
    measure: str = ""
    measure_id: str = ""
    short_question_text: str = ""
    data_source: str = ""
    data_value_type: str = ""
    data_value_type_id: str = ""
    value_unit: str = ""
    data_value: float | None = None
    confidence_interval: dict[str, float | None] = Field(default_factory=dict)
    population: dict[str, int | None] = Field(default_factory=dict)
    geolocation: dict[str, float] | None = None
    source: dict[str, Any] | None = None
    notes: list[str] = Field(default_factory=list)


class MeasureMetadata(BaseModel):
    measure_id: str
    measure: str
    short_question_text: str = ""
    category: str = ""
    category_id: str = ""
    data_value_type: str = ""
    data_value_type_id: str = ""
    value_unit: str = ""
    source_note: str


class LocationSummary(BaseModel):
    location_id: str
    location_name: str
    geography_type: str
    state_abbr: str | None = None
    state_name: str | None = None
    population: dict[str, int | None] = Field(default_factory=dict)
    geolocation: dict[str, float] | None = None


class PlacesProfile(BaseModel):
    location: LocationSummary | None = None
    measures: list[PlacesMeasure] = Field(default_factory=list)
    missing_data_notes: list[str] = Field(default_factory=list)
    interpretation: str


class MarketCommunityProfile(BaseModel):
    geographic_basis: list[str] = Field(default_factory=list)
    locations: list[LocationSummary] = Field(default_factory=list)
    aggregated_measures: list[dict[str, Any]] = Field(default_factory=list)
    missing_data_notes: list[str] = Field(default_factory=list)
    interpretation: str

