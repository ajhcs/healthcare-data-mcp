# Physician & Referral Network — Design Document

**Server:** Port 8010, name `physician-referral-network`
**Goal:** Provide physician search, profiles, referral network mapping, employment mix analysis, and referral leakage detection for health system strategy analysis.

---

## Architecture

Four-module server:

| Module | Responsibility | Primary Data Sources |
|--------|---------------|---------------------|
| `nppes_client.py` | Physician search & profiles | NPPES API (real-time), Physician Compare CSV, Medicare Utilization PUF |
| `referral_network.py` | Referral graph + leakage detection | DocGraph shared patient data (2014-2020), Dartmouth Atlas HSA crosswalks |
| `physician_mix.py` | Employment status classification | NPPES org-linkage, AHRQ Compendium, CMS POS |
| `server.py` + `models.py` | FastMCP tools + Pydantic models | — |

**Shared dependency:** `shared/utils/cms_client.py` for NPPES lookups and CMS data downloads.

---

## Data Sources

### Real-Time APIs (no auth, no rate limits)

| API | Endpoint | Used By |
|-----|----------|---------|
| NPPES NPI Registry | `https://npiregistry.cms.hhs.gov/api/?version=2.1` | search_physicians, get_physician_profile, analyze_physician_mix |

### Bulk Downloads (cached as Parquet, 30-day TTL)

| Dataset | Source | Cache Path | Approx Size |
|---------|--------|-----------|-------------|
| Physician Compare National | data.cms.gov `mj5m-pzi6` | `~/.healthcare-data-mcp/cache/physician/physician_compare.parquet` | ~500MB CSV → ~150MB Parquet |
| Medicare Utilization PUF | data.cms.gov (Physician & Other Supplier) | `~/.healthcare-data-mcp/cache/physician/utilization.parquet` | ~2-4GB CSV → ~800MB Parquet |
| DocGraph Shared Patient | CareSet/DocGraph (2014-2020) | `~/.healthcare-data-mcp/cache/docgraph/shared_patients.parquet` | ~500MB-1GB CSV → ~200MB Parquet |
| AHRQ Compendium | ahrq.gov (reuse health-system-profiler loader) | `~/.healthcare-data-mcp/cache/ahrq/` | Already cached by health-system-profiler |
| CMS POS | data.cms.gov (reuse health-system-profiler loader) | `~/.healthcare-data-mcp/cache/pos/` | Already cached by health-system-profiler |
| Dartmouth Atlas HSA/HRR | dartmouthatlas.org | `~/.healthcare-data-mcp/cache/dartmouth/hsa_crosswalk.parquet` | ~5MB |

---

## Module 1: nppes_client.py

### Functions

**`search_physicians(query, specialty="", state="", limit=25) -> list[dict]`**
- Calls `shared.utils.cms_client.nppes_lookup` with `enumeration_type=NPI-1`
- Filters by `taxonomy_description` if specialty provided
- Returns NPI, name, specialty, practice location, organization

**`get_physician_detail(npi) -> dict`**
- Single-NPI NPPES lookup for demographics/affiliations
- Enriches with Physician Compare data (quality, group practice)
- Enriches with Medicare Utilization PUF (services, payments)

**`get_utilization_summary(npi) -> dict | None`**
- Queries cached utilization.parquet by NPI via DuckDB
- Returns: total_services, total_beneficiaries, total_medicare_payment, avg_allowed_amount, top_hcpcs_codes

**`get_quality_scores(npi) -> dict | None`**
- Queries cached physician_compare.parquet by NPI
- Returns: group_practice_pac_id, quality_score_measures, graduation_year, hospital_affiliations

**`ensure_physician_data_cached() -> None`**
- Downloads Physician Compare CSV and Utilization PUF if not cached
- Converts to Parquet with zstd compression
- 30-day TTL check

---

## Module 2: referral_network.py

### DocGraph Data

The DocGraph Hop Teaming dataset contains directed edges of physician pairs who share Medicare patients. Each row represents a pair of NPIs with shared patient counts over a time period.

**Parquet schema:**
```
npi_from (str)    — referring/originating NPI
npi_to (str)      — receiving NPI
shared_count (int) — number of shared patients
transaction_count (int) — number of shared transactions
same_day_count (int) — same-day encounters
year (int)        — data year
```

### Functions

**`ensure_docgraph_cached() -> bool`**
- Downloads DocGraph shared patient CSV if not cached
- Converts to Parquet
- Returns True if data available, False if download fails (with warning)

**`get_referral_network(npi, depth=1, min_shared=11) -> dict`**
- DuckDB query: all NPIs sharing >= min_shared patients with target NPI
- If depth=2, extends to second-hop connections
- Returns nodes (NPIs with names from NPPES) and edges (shared counts)
- Output designed for graph visualization

**`get_top_referral_pairs(npi, direction="both", limit=25) -> list[dict]`**
- `direction="outgoing"`: NPIs this physician refers TO (npi_from=target)
- `direction="incoming"`: NPIs that refer TO this physician (npi_to=target)
- `direction="both"`: all connections sorted by shared_count
- Enriches each NPI with name/specialty from NPPES

**`detect_leakage(system_npis, system_zips, min_shared=11) -> dict`**
- Takes set of in-network NPIs and HSA ZIP codes
- Queries DocGraph for all outbound referrals from system_npis
- Classifies each referral target:
  - In-network: target NPI is in system_npis set
  - Out-of-network/in-area: target NPI not in system but in same HSA ZIPs
  - Out-of-network/out-of-area: target outside both network and area
- Groups leakage by specialty (taxonomy lookup)
- Returns leakage rate, top leakage destinations, specialty breakdown

### Dartmouth Atlas Integration

Downloads HSA/HRR crosswalk CSV from `data.dartmouthatlas.org`:
- ZIP → HSA mapping
- ZIP → HRR mapping
- Used to define geographic service area boundaries for leakage detection

---

## Module 3: physician_mix.py

### Classification Algorithm

For a given health system, classify each physician as employed, affiliated, or independent:

1. **Get system facilities:** AHRQ Compendium → CCNs → CMS POS → addresses
2. **Get system physicians:** NPPES search by system name + state → NPI-1 records
3. **Classify each physician:**
   - Practice address matches facility address → **employed** (confidence: 0.9)
   - Org name in NPI record matches system name → **affiliated** (confidence: 0.7)
   - Same HSA as system facility but no org match → **independent** (confidence: 0.5)
   - Different HSA, no match → **unrelated** (excluded from results)

### Functions

**`classify_physician(npi, system_facilities) -> dict`**
- Returns `{status, confidence, evidence: [reasons]}`
- Evidence explains why the classification was made

**`analyze_system_mix(system_name, state="") -> dict`**
- Batch process: find all physicians → classify each → aggregate
- Returns: employed_count, affiliated_count, independent_count, percentages, specialty breakdown
- Caches results for 24 hours (expensive operation)

---

## Pydantic Models

```
PhysicianSummary: npi, name, specialty, city, state, org_name
PhysicianSearchResponse: total_results, physicians: list[PhysicianSummary]

PhysicianProfile: npi, name, specialties, practice_locations, org_affiliations,
                  graduation_year, quality_scores, utilization_summary

ReferralNode: npi, name, specialty, city, state
ReferralEdge: npi_from, npi_to, shared_count, direction
ReferralNetworkResponse: center_npi, center_name, nodes, edges, total_connections

LeakageDestination: npi, name, specialty, shared_count, location, classification
LeakageResponse: system_name, total_referrals, in_network_pct, out_of_network_in_area_pct,
                 out_of_area_pct, top_leakage_destinations, specialty_breakdown

PhysicianClassification: npi, name, specialty, status, confidence, evidence
PhysicianMixResponse: system_name, total_physicians, employed, affiliated, independent,
                      employed_pct, affiliated_pct, independent_pct, by_specialty
```

---

## Tool Signatures

| Tool | Parameters | Returns |
|------|-----------|---------|
| `search_physicians(query, specialty="", state="", limit=25)` | name/NPI/org query | PhysicianSearchResponse |
| `get_physician_profile(npi)` | 10-digit NPI | PhysicianProfile |
| `map_referral_network(npi, depth=1, min_shared=11)` | center NPI | ReferralNetworkResponse |
| `analyze_physician_mix(system_name, state="")` | health system name | PhysicianMixResponse |
| `detect_leakage(system_name, state="", specialty="")` | system + optional filters | LeakageResponse |

---

## Data Freshness & Limitations

- **NPPES:** Real-time (updated weekly by CMS)
- **Physician Compare:** Quarterly updates, 30-day cache
- **Utilization PUF:** Annual release (2024 data most recent), 30-day cache
- **DocGraph:** 2014-2020 data only (clearly labeled in responses as historical baseline)
- **AHRQ Compendium:** 2023 release (reused from health-system-profiler)
- **Dartmouth Atlas:** Updated periodically, stable HSA/HRR definitions

**Key limitation:** DocGraph data is 4-6 years old. Referral network and leakage results should be presented as "historical baseline patterns" not current state. All responses include data vintage metadata.

---

## Error Handling

- All tools wrap in try/except, return error JSON
- DocGraph download failure: tools gracefully degrade (return "referral data not available")
- NPPES timeout: 30s per request, retry once
- Bulk data download: progress logging, resume on failure if partial download
- Classification confidence < 0.5: excluded from results or flagged
