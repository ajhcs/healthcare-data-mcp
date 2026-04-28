"""Health System Data Collector.

Aggregates data from all sources for a single health system into a
structured JSON payload ready for report generation.
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from tools.systems import get_system, HealthSystem
from tools.validation import validate_system_key
from tools.cms_quality import get_all_cms_data
from tools.propublica import get_health_system_financials
from tools.nppes import get_health_system_providers


async def collect_system_data(system_key: str) -> dict[str, Any]:
    """Collect all available data for a health system from all sources.

    Runs API calls concurrently for speed. Returns a structured dict
    with sections for each data source.
    """
    system_key = validate_system_key(system_key)
    system = get_system(system_key)

    print(f"Collecting data for {system.name}...")
    print(f"  CMS Facility ID: {system.cms_facility_id}")
    print(f"  EIN: {system.ein}")
    print(f"  Primary NPI: {system.primary_npi}")

    # Run all data collection concurrently
    cms_task = get_all_cms_data(system.cms_facility_id)
    financial_task = get_health_system_financials(system_key)
    provider_task = get_health_system_providers(system_key)

    print("  Fetching CMS quality data...")
    print("  Fetching 990 financial data...")
    print("  Fetching NPI provider data...")

    cms_data, financial_data, provider_data = await asyncio.gather(
        cms_task, financial_task, provider_task,
        return_exceptions=True,
    )

    # Build the payload
    payload = {
        "metadata": {
            "system_key": system_key,
            "system_name": system.name,
            "short_name": system.short_name,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
            "product": "Open-Informatics Health System Intelligence Report",
        },
        "system_profile": {
            "name": system.name,
            "short_name": system.short_name,
            "city": system.city,
            "state": system.state,
            "system_type": system.system_type,
            "description": system.description,
            "identifiers": {
                "cms_facility_id": system.cms_facility_id,
                "ein": system.ein,
                "primary_npi": system.primary_npi,
            },
        },
        "cms_quality_data": cms_data if not isinstance(cms_data, Exception) else {"error": str(cms_data)},
        "financial_data": financial_data if not isinstance(financial_data, Exception) else {"error": str(financial_data)},
        "provider_data": provider_data if not isinstance(provider_data, Exception) else {"error": str(provider_data)},
    }

    # Summary stats
    _print_summary(payload)

    return payload


def _print_summary(payload: dict):
    """Print a summary of collected data."""
    print("\n  Collection Summary:")

    # CMS data
    cms = payload.get("cms_quality_data", {})
    if "error" not in cms:
        cms_sections = cms.get("cms_data", {})
        for section, data in cms_sections.items():
            count = data.get("record_count", 0) if isinstance(data, dict) else 0
            print(f"    CMS {section}: {count} records")
    else:
        print(f"    CMS: ERROR - {cms['error']}")

    # Financial data
    fin = payload.get("financial_data", {})
    if "error" not in fin:
        entities = fin.get("entities", [])
        print(f"    990 Financials: {len(entities)} entities")
        for e in entities:
            filings = len(e.get("recent_financials", []))
            print(f"      {e.get('name', 'Unknown')}: {filings} years of data")
    else:
        print(f"    Financials: ERROR - {fin['error']}")

    # Provider data
    prov = payload.get("provider_data", {})
    if "error" not in prov:
        orgs = prov.get("affiliated_organizations", {})
        count = orgs.get("result_count", 0) if isinstance(orgs, dict) else 0
        print(f"    NPI Organizations: {count} affiliated NPIs")
        tax = prov.get("taxonomy_summary", {})
        specialties = len(tax.get("taxonomy_distribution", [])) if isinstance(tax, dict) else 0
        print(f"    Specialties: {specialties} distinct types")
    else:
        print(f"    Providers: ERROR - {prov['error']}")


async def collect_and_save(system_key: str, output_dir: str = "output") -> Path:
    """Collect data and save to JSON file."""
    data = await collect_system_data(system_key)

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    filename = f"{system_key}_data_{datetime.now().strftime('%Y%m%d')}.json"
    filepath = out_path / filename

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n  Saved to: {filepath}")
    print(f"  File size: {filepath.stat().st_size / 1024:.1f} KB")

    return filepath


async def collect_all_systems(output_dir: str = "output") -> list[Path]:
    """Collect data for all configured health systems."""
    from tools.systems import SYSTEMS

    paths = []
    for key in SYSTEMS:
        print(f"\n{'='*60}")
        path = await collect_and_save(key, output_dir)
        paths.append(path)

    return paths


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        system = sys.argv[1]
        asyncio.run(collect_and_save(system))
    else:
        asyncio.run(collect_all_systems())
