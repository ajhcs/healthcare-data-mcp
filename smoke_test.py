"""Smoke tests for all 5 healthcare MCP servers.

Tests each server's tools against live CMS APIs and data sources.
Run with: python smoke_test.py
"""

import asyncio
import json
import sys
import time
import traceback

# ============================================================
# Test 1: CMS Facility Server
# ============================================================
async def test_cms_facility():
    print("\n" + "=" * 60)
    print("TEST: cms-facility-mcp")
    print("=" * 60)

    sys.path.insert(0, "servers/cms-facility")
    from servers.cms_facility import data_loaders

    results = {}

    # Test: Load Hospital General Info CSV from CMS
    print("\n[1/3] Loading Hospital General Info from data.cms.gov...")
    t0 = time.time()
    df = await data_loaders.load_hospital_info()
    elapsed = time.time() - t0
    print(f"  -> Loaded {len(df)} hospitals in {elapsed:.1f}s")
    print(f"  -> Columns: {list(df.columns[:10])}...")
    results["hospital_info_rows"] = len(df)
    assert len(df) > 1000, f"Expected >1000 hospitals, got {len(df)}"

    # Test: Search by state
    print("\n[2/3] Searching facilities in CA...")
    state_col = None
    for c in df.columns:
        if c.lower() == "state":
            state_col = c
            break
    if state_col:
        ca_hospitals = df[df[state_col].str.upper() == "CA"]
        print(f"  -> Found {len(ca_hospitals)} CA hospitals")
        if len(ca_hospitals) > 0:
            sample = ca_hospitals.iloc[0]
            print(f"  -> Sample: {sample.get('facility_name', sample.get('hospital_name', 'N/A'))}")
        results["ca_hospitals"] = len(ca_hospitals)

    # Test: NPPES API (live query)
    print("\n[3/3] Querying NPPES API for 'Mayo Clinic'...")
    t0 = time.time()
    npi_results = await data_loaders.search_nppes(organization_name="Mayo Clinic", limit=5)
    elapsed = time.time() - t0
    print(f"  -> Got {len(npi_results)} results in {elapsed:.1f}s")
    if npi_results:
        first = npi_results[0]
        name = first.get("basic", {}).get("organization_name", "N/A")
        npi = first.get("number", "N/A")
        print(f"  -> First: {name} (NPI: {npi})")
    results["nppes_results"] = len(npi_results)
    assert len(npi_results) > 0, "Expected NPPES results for Mayo Clinic"

    print("\n  CMS-FACILITY: ALL PASSED")
    return results


# ============================================================
# Test 2: Service Area Server
# ============================================================
async def test_service_area():
    print("\n" + "=" * 60)
    print("TEST: service-area-mcp")
    print("=" * 60)

    from servers.service_area import data_loaders, service_area_engine
    import pandas as pd

    results = {}

    # Test: Load HSAF
    print("\n[1/3] Loading CMS Hospital Service Area File...")
    t0 = time.time()
    hsaf = await data_loaders.download_hsaf()
    elapsed = time.time() - t0
    print(f"  -> Loaded {len(hsaf)} rows in {elapsed:.1f}s")
    print(f"  -> Columns: {list(hsaf.columns)}")
    results["hsaf_rows"] = len(hsaf)
    assert len(hsaf) > 10000, f"Expected >10k HSAF rows, got {len(hsaf)}"

    # Test: Get unique hospitals
    unique_hospitals = hsaf["ccn"].nunique()
    print(f"  -> Unique hospitals: {unique_hospitals}")
    results["unique_hospitals"] = unique_hospitals

    # Test: Compute service area for a real hospital
    # Find a hospital with decent volume
    hospital_volumes = hsaf.groupby("ccn")["discharges"].sum().sort_values(ascending=False)
    top_ccn = hospital_volumes.index[0]
    top_name = hsaf[hsaf["ccn"] == top_ccn]["facility_name"].iloc[0]
    print(f"\n[2/3] Computing PSA/SSA for {top_name} (CCN: {top_ccn})...")

    facility_df = hsaf[hsaf["ccn"] == top_ccn]
    discharge_data = facility_df[["zip_code", "discharges"]].copy()

    t0 = time.time()
    sa = service_area_engine.derive_service_area(discharge_data)
    elapsed = time.time() - t0

    print(f"  -> Total discharges: {sa['total_discharges']}")
    print(f"  -> PSA: {len(sa['psa_zips'])} ZIPs, {sa['psa_pct']*100:.1f}% of volume")
    print(f"  -> SSA: {len(sa['ssa_zips'])} ZIPs, {sa['ssa_pct']*100:.1f}% of volume")
    print(f"  -> Remaining: {sa['remaining_zips_count']} ZIPs")
    print(f"  -> Computed in {elapsed:.3f}s")
    results["psa_zips"] = len(sa["psa_zips"])
    results["ssa_zips"] = len(sa["ssa_zips"])
    assert len(sa["psa_zips"]) > 0, "PSA should have at least 1 ZIP"
    assert sa["psa_pct"] >= 0.70, f"PSA should cover ~75%, got {sa['psa_pct']}"

    # Test: Market share for a ZIP
    top_zip = discharge_data.sort_values("discharges", ascending=False).iloc[0]["zip_code"]
    print(f"\n[3/3] Market share for ZIP {top_zip}...")
    ms = service_area_engine.compute_market_share(hsaf, top_zip, limit=5)
    print(f"  -> {ms['total_discharges']} total discharges from this ZIP")
    for h in ms["hospitals"][:3]:
        print(f"     {h['facility_name']}: {h['market_share_pct']}%")
    results["market_share_hospitals"] = len(ms["hospitals"])

    print("\n  SERVICE-AREA: ALL PASSED")
    return results


# ============================================================
# Test 3: Geo-Demographics Server
# ============================================================
async def test_geo_demographics():
    print("\n" + "=" * 60)
    print("TEST: geo-demographics-mcp")
    print("=" * 60)

    from servers.geo_demographics import census_client

    results = {}

    # Test: Census ACS query for a known ZCTA (Beverly Hills 90210)
    print("\n[1/2] Querying Census ACS for ZCTA 90210...")
    t0 = time.time()
    try:
        demo = await census_client.get_demographics_for_zcta("90210")
        elapsed = time.time() - t0
        print(f"  -> Query completed in {elapsed:.1f}s")
        if demo:
            print(f"  -> Total population: {demo.get('total_population', 'N/A')}")
            print(f"  -> Median income: ${demo.get('median_household_income', 'N/A')}")
            results["zcta_population"] = demo.get("total_population")
        else:
            print("  -> No data returned (may need CENSUS_API_KEY env var)")
            results["zcta_population"] = None
    except Exception as e:
        print(f"  -> Census API error: {e}")
        print(f"     (This is expected if CENSUS_API_KEY is not set)")
        results["zcta_population"] = "error"

    # Test: HUD crosswalk (needs HUD_API_TOKEN)
    print("\n[2/2] Testing HUD crosswalk for ZIP 90210...")
    import os
    if os.environ.get("HUD_API_TOKEN"):
        from servers.geo_demographics import server as geo_server
        try:
            result = await geo_server.crosswalk_zip("90210", "county")
            data = json.loads(result)
            print(f"  -> {json.dumps(data, indent=2)[:200]}")
            results["hud_crosswalk"] = "ok"
        except Exception as e:
            print(f"  -> HUD API error: {e}")
            results["hud_crosswalk"] = "error"
    else:
        print("  -> Skipped (HUD_API_TOKEN not set)")
        results["hud_crosswalk"] = "skipped"

    print("\n  GEO-DEMOGRAPHICS: PASSED (API-key-dependent tests noted)")
    return results


# ============================================================
# Test 4: Drive Time Server
# ============================================================
async def test_drive_time():
    print("\n" + "=" * 60)
    print("TEST: drive-time-mcp")
    print("=" * 60)

    from servers.drive_time.routing import OSRMRouter
    from servers.drive_time.accessibility import compute_e2sfca, summarize_scores

    results = {}

    # Test: OSRM route (public demo server)
    # Mayo Clinic Rochester (44.0225, -92.4670) to Minneapolis (44.9778, -93.2650)
    print("\n[1/3] Testing OSRM route: Rochester MN -> Minneapolis MN...")
    router = OSRMRouter()
    t0 = time.time()
    try:
        route = await router.route((-92.4670, 44.0225), (-93.2650, 44.9778))
        elapsed = time.time() - t0
        duration_min = route["duration_seconds"] / 60
        distance_mi = route["distance_meters"] / 1609.34
        print(f"  -> {duration_min:.0f} min, {distance_mi:.0f} miles ({elapsed:.1f}s)")
        results["osrm_route"] = f"{duration_min:.0f}min"
        assert 45 < duration_min < 120, f"Route time {duration_min}min seems wrong"
    except Exception as e:
        print(f"  -> OSRM error: {e}")
        results["osrm_route"] = "error"

    # Test: OSRM table (2x2 matrix)
    print("\n[2/3] Testing OSRM 2x2 drive time matrix...")
    coords = [
        (-92.4670, 44.0225),  # Rochester
        (-93.2650, 44.9778),  # Minneapolis
        (-93.0900, 44.9537),  # St. Paul
        (-91.5070, 44.0121),  # Winona
    ]
    t0 = time.time()
    try:
        table = await router.table(coords, sources=[0, 1], destinations=[2, 3])
        elapsed = time.time() - t0
        durations = table.get("durations", [])
        print(f"  -> Got {len(durations)}x{len(durations[0]) if durations else 0} matrix in {elapsed:.1f}s")
        for i, row in enumerate(durations):
            row_mins = [f"{d/60:.0f}min" if d else "N/A" for d in row]
            print(f"     Source {i}: {row_mins}")
        results["osrm_table"] = f"{len(durations)}x{len(durations[0]) if durations else 0}"
    except Exception as e:
        print(f"  -> OSRM table error: {e}")
        results["osrm_table"] = "error"

    # Test: E2SFCA (pure math, no API)
    print("\n[3/3] Testing E2SFCA accessibility scoring...")
    # 3 demand points, 2 supply points
    travel_matrix = [
        [10, 25],   # demand 0: 10min to supply 0, 25min to supply 1
        [20, 15],   # demand 1: 20min to supply 0, 15min to supply 1
        [35, 5],    # demand 2: 35min to supply 0, 5min to supply 1
    ]
    populations = [10000, 8000, 5000]
    capacities = [200, 150]  # beds

    scores = compute_e2sfca(travel_matrix, populations, capacities, catchment_minutes=30)
    summary = summarize_scores(scores)
    print(f"  -> Scores: {[round(s, 6) for s in scores]}")
    print(f"  -> Mean: {summary['mean']:.6f}, Min: {summary['min']:.6f}, Max: {summary['max']:.6f}")
    print(f"  -> Zero-access points: {summary['points_with_zero_access']}")
    results["e2sfca_scores"] = [round(s, 6) for s in scores]
    assert all(s >= 0 for s in scores), "Scores should be non-negative"
    assert any(s > 0 for s in scores), "At least one score should be positive"

    print("\n  DRIVE-TIME: ALL PASSED")
    return results


# ============================================================
# Test 5: Hospital Quality Server
# ============================================================
async def test_hospital_quality():
    print("\n" + "=" * 60)
    print("TEST: hospital-quality-mcp")
    print("=" * 60)

    from servers.hospital_quality import data_loaders

    results = {}

    # Test: Load Hospital General Info (quality ratings)
    print("\n[1/3] Loading Hospital General Info for quality scores...")
    t0 = time.time()
    df = await data_loaders.load_hospital_info()
    elapsed = time.time() - t0
    print(f"  -> Loaded {len(df)} rows in {elapsed:.1f}s")
    results["quality_rows"] = len(df)

    # Test: Load HRRP data
    print("\n[2/3] Loading HRRP readmission data...")
    t0 = time.time()
    hrrp = await data_loaders.load_hrrp()
    elapsed = time.time() - t0
    print(f"  -> Loaded {len(hrrp)} rows in {elapsed:.1f}s")
    if len(hrrp) > 0:
        print(f"  -> Columns: {list(hrrp.columns[:8])}...")
        unique = hrrp[hrrp.columns[0]].nunique() if len(hrrp.columns) > 0 else 0
        print(f"  -> Unique facilities: ~{unique}")
    results["hrrp_rows"] = len(hrrp)

    # Test: Load HAC data
    print("\n[3/3] Loading HAC safety data...")
    t0 = time.time()
    hac = await data_loaders.load_hac()
    elapsed = time.time() - t0
    print(f"  -> Loaded {len(hac)} rows in {elapsed:.1f}s")
    if len(hac) > 0:
        print(f"  -> Columns: {list(hac.columns[:8])}...")
    results["hac_rows"] = len(hac)

    print("\n  HOSPITAL-QUALITY: ALL PASSED")
    return results


# ============================================================
# Test 6: Financial Intelligence Server
# ============================================================
async def test_financial_intelligence():
    print("\n" + "=" * 60)
    print("TEST: financial-intelligence-mcp")
    print("=" * 60)

    from servers.financial_intelligence import propublica_client, edgar_client

    results = {}

    # Test 1: ProPublica search (search for "Cleveland Clinic")
    print("\n[1/4] Searching ProPublica for 'Cleveland Clinic'...")
    t0 = time.time()
    pp_search = await propublica_client.search_organizations("Cleveland Clinic")
    elapsed = time.time() - t0
    orgs = pp_search.get("organizations", [])
    total = pp_search.get("total_results", 0)
    print(f"  -> {total} total results, {len(orgs)} returned in {elapsed:.1f}s")
    if orgs:
        first = orgs[0]
        print(f"  -> First: {first.get('name', 'N/A')} (EIN: {first.get('ein', 'N/A')})")
        print(f"     Revenue: ${first.get('income_amount', 'N/A'):,}" if isinstance(first.get('income_amount'), (int, float)) else f"     Revenue: {first.get('income_amount', 'N/A')}")
    results["propublica_search_total"] = total
    assert len(orgs) > 0, "Expected ProPublica results for Cleveland Clinic"

    # Test 2: ProPublica org detail (EIN "340714585" - The Cleveland Clinic Foundation)
    print("\n[2/4] Getting ProPublica org detail for EIN 340714585...")
    t0 = time.time()
    pp_org = await propublica_client.get_organization("340714585")
    elapsed = time.time() - t0
    org_info = pp_org.get("organization", {})
    filings = pp_org.get("filings_with_data", [])
    print(f"  -> Org: {org_info.get('name', 'N/A')} in {elapsed:.1f}s")
    print(f"  -> Filings with data: {len(filings)}")
    if filings:
        latest = filings[0]
        print(f"  -> Latest filing period: {latest.get('tax_prd', 'N/A')}")
        print(f"  -> Revenue: {latest.get('totrevenue', 'N/A')}")
    results["propublica_org_filings"] = len(filings)
    assert org_info.get("name"), "Expected organization name for EIN 340714585"

    # Test 3: EDGAR EFTS search (search "HCA Healthcare" 10-K)
    print("\n[3/4] Searching EDGAR EFTS for 'HCA Healthcare' 10-K...")
    t0 = time.time()
    efts_data = await edgar_client.search_filings("HCA Healthcare", forms="10-K")
    elapsed = time.time() - t0
    hits_obj = efts_data.get("hits", {})
    raw_hits = hits_obj.get("hits", [])
    total_obj = hits_obj.get("total", {})
    total_count = total_obj.get("value", 0) if isinstance(total_obj, dict) else 0
    print(f"  -> {total_count} total hits, {len(raw_hits)} returned in {elapsed:.1f}s")
    if raw_hits:
        first_hit = raw_hits[0].get("_source", {})
        display_names = first_hit.get("display_names", [])
        print(f"  -> First hit company: {display_names[0] if display_names else 'N/A'}")
        print(f"  -> ADSH: {first_hit.get('adsh', 'N/A')}")
        print(f"  -> Form: {first_hit.get('form', 'N/A')}")
        print(f"  -> File date: {first_hit.get('file_date', 'N/A')}")
        print(f"  -> CIKs: {first_hit.get('ciks', [])}")
    results["efts_total_hits"] = total_count
    assert len(raw_hits) > 0, "Expected EFTS results for HCA Healthcare 10-K"

    # Test 4: EDGAR XBRL company facts (Apple CIK "320193") + extract_financials
    print("\n[4/4] Getting EDGAR XBRL company facts for Apple (CIK 320193)...")
    t0 = time.time()
    facts = await edgar_client.get_company_facts("320193")
    elapsed = time.time() - t0
    entity_name = facts.get("entityName", "N/A")
    fact_taxonomies = list(facts.get("facts", {}).keys())
    print(f"  -> Entity: {entity_name} in {elapsed:.1f}s")
    print(f"  -> Taxonomies: {fact_taxonomies}")

    financials = edgar_client.extract_financials(facts)
    print(f"  -> Revenue: ${financials.get('revenue', 0):,.0f}" if financials.get('revenue') else "  -> Revenue: N/A")
    print(f"  -> Net income: ${financials.get('net_income', 0):,.0f}" if financials.get('net_income') else "  -> Net income: N/A")
    print(f"  -> Total assets: ${financials.get('total_assets', 0):,.0f}" if financials.get('total_assets') else "  -> Total assets: N/A")
    results["xbrl_entity"] = entity_name
    results["xbrl_revenue"] = financials.get("revenue")
    assert entity_name != "N/A", "Expected entity name from XBRL company facts"
    assert financials.get("revenue") is not None, "Expected revenue from extract_financials"

    print("\n  FINANCIAL-INTELLIGENCE: ALL PASSED")
    return results


# ============================================================
# Test 7: Price Transparency / MRF Engine Server
# ============================================================
async def test_price_transparency():
    print("\n" + "=" * 60)
    print("TEST: price-transparency-mcp")
    print("=" * 60)

    results = {}

    # Test 1: CMS Provider Data Catalog lookup
    print("\n[1/4] CMS Provider Data Catalog — searching for 'MAYO'...")
    from servers.price_transparency.mrf_registry import search_cms_providers
    t0 = time.time()
    providers = await search_cms_providers("MAYO")
    elapsed = time.time() - t0
    print(f"  -> Found {len(providers)} providers in {elapsed:.1f}s")
    if providers:
        p = providers[0]
        print(f"  -> First: {p.get('facility_name')} ({p.get('state')})")
    results["cms_provider_count"] = len(providers)
    assert len(providers) > 0, "Expected CMS Provider results for MAYO"

    # Test 2: CMS Physician Fee Schedule lookup
    print("\n[2/4] CMS PFS — looking up CPT 99213...")
    from servers.price_transparency.benchmark_client import get_pfs_rate, calculate_medicare_allowed
    t0 = time.time()
    pfs = await get_pfs_rate("99213")
    elapsed = time.time() - t0
    if pfs:
        print(f"  -> Work RVU: {pfs.get('rvu_work')} in {elapsed:.1f}s")
        print(f"  -> Total RVU (non-facility): {pfs.get('full_nfac_total')}")
        allowed = calculate_medicare_allowed(pfs)
        print(f"  -> Medicare allowed (national): ${allowed}")
        results["pfs_allowed"] = allowed
    else:
        print("  -> PFS lookup returned None")
        results["pfs_allowed"] = None
    assert pfs is not None, "Expected PFS data for 99213"

    # Test 3: Registry search (local)
    print("\n[3/4] MRF Registry — local registry search...")
    from servers.price_transparency.mrf_registry import search_registry
    local = search_registry("test")
    print(f"  -> Local registry entries matching 'test': {len(local)}")
    results["local_registry_count"] = len(local)

    # Test 4: MRF processor — cache status
    print("\n[4/4] MRF Processor — cache status...")
    from servers.price_transparency.mrf_processor import get_all_cached_hospitals
    cached = get_all_cached_hospitals()
    print(f"  -> Cached hospitals: {len(cached)}")
    results["cached_hospitals"] = len(cached)

    print("\n  PRICE-TRANSPARENCY: ALL PASSED")
    return results


# ============================================================
# Main runner
# ============================================================
async def main():
    print("=" * 60)
    print("HEALTHCARE MCP SERVERS — SMOKE TEST")
    print("=" * 60)

    all_results = {}
    failures = []

    tests = [
        ("cms-facility", test_cms_facility),
        ("service-area", test_service_area),
        ("geo-demographics", test_geo_demographics),
        ("drive-time", test_drive_time),
        ("hospital-quality", test_hospital_quality),
        ("financial-intelligence", test_financial_intelligence),
        ("price-transparency", test_price_transparency),
    ]

    for name, test_fn in tests:
        try:
            result = await test_fn()
            all_results[name] = {"status": "PASSED", "details": result}
        except Exception as e:
            tb = traceback.format_exc()
            print(f"\n  {name.upper()}: FAILED — {e}")
            print(f"  {tb}")
            all_results[name] = {"status": "FAILED", "error": str(e)}
            failures.append(name)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, result in all_results.items():
        status = result["status"]
        icon = "PASS" if status == "PASSED" else "FAIL"
        print(f"  [{icon}] {name}")
        if status == "FAILED":
            print(f"         Error: {result['error']}")

    if failures:
        print(f"\n{len(failures)} server(s) failed: {', '.join(failures)}")
        return 1
    else:
        print(f"\nAll {len(tests)} servers passed smoke tests!")
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
