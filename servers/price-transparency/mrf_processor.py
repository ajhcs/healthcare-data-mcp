"""MRF Processor -- CSV/JSON parsing, download, Parquet normalization, DuckDB queries.

Combines:
- CMS column mapping + fuzzy column recognition (adapted from MR-Explore)
- CSV MRF parsing with header detection and wide-to-tall pivot
- JSON MRF parsing for in_network / allowed_amounts sections
- Async MRF file download via httpx streaming
- Parquet normalization with lookup tables
- DuckDB query interface over cached Parquet
"""

from __future__ import annotations

import csv
import json
import logging
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache layout
# ---------------------------------------------------------------------------

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "mrf"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_CACHE_TTL_DAYS = 30


def _hospital_cache_dir(hospital_id: str) -> Path:
    """Return cache directory for a hospital, using a filesystem-safe ID."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", hospital_id.strip())
    return _CACHE_DIR / safe_id


def is_cached(hospital_id: str) -> bool:
    """Check whether valid Parquet cache exists for *hospital_id*."""
    d = _hospital_cache_dir(hospital_id)
    charges = d / "charges.parquet"
    meta = d / "metadata.json"
    if not (charges.exists() and meta.exists()):
        return False
    try:
        info = json.loads(meta.read_text(encoding="utf-8"))
        cached_ts = info.get("cached_at", "")
        if cached_ts:
            cached_dt = datetime.fromisoformat(cached_ts)
            age_days = (datetime.now(timezone.utc) - cached_dt).days
            if age_days > _CACHE_TTL_DAYS:
                return False
    except Exception:
        return False
    return True


# ---------------------------------------------------------------------------
# CMS column mapping
# ---------------------------------------------------------------------------

CMS_COLUMN_MAPPING: dict[str, str] = {
    "Description": "description",
    "Code|1": "code_1",
    "Code|1|Type": "code_1_type",
    "Code|2": "code_2",
    "Code|2|Type": "code_2_type",
    "Modifiers": "modifiers",
    "Setting": "setting",
    "Drug_Unit_Of_Measurement": "drug_unit_of_measurement",
    "Drug_Type_Of_Measurement": "drug_type_of_measurement",
    "Standard_Charge|Gross": "gross_charge",
    "Standard_Charge|Discounted_Cash": "discounted_cash",
    "Payer_Name": "payer_name",
    "Plan_Name": "plan_name",
    "Standard_Charge|Negotiated_Dollar": "negotiated_dollar",
    "Standard_Charge|Negotiated_Percentage": "negotiated_percentage",
    "Standard_Charge|Negotiated_Algorithm": "negotiated_algorithm",
    "Estimated_Amount": "estimated_amount",
    "Standard_Charge|Methodology": "methodology",
    "Standard_Charge|Min": "min_charge",
    "Standard_Charge|Max": "max_charge",
    "Additional_Generic_Notes": "additional_notes",
    "Billing_Class": "billing_class",
}

# ---------------------------------------------------------------------------
# Fuzzy column recognition
# ---------------------------------------------------------------------------

_SKIP_PATTERNS: list[tuple[re.Pattern, None]] = [
    (re.compile(r"\b(hospital|facility)[_ ]?(name)?\b", re.IGNORECASE), None),
]

_KEYWORD_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(description|procedure|service_description|item_description|service)\b", re.I), "description"),
    (re.compile(r"\b(code[_ ]?type|type[_ ]?of[_ ]?code)\b", re.I), "code_1_type"),
    (re.compile(r"\b(cpt[_ ]?code|hcpcs[_ ]?code|procedure[_ ]?code|cpt|hcpcs|code)\b", re.I), "code_1"),
    (re.compile(r"\b(payer|payer[_ ]?name|insurer|insurance|insurance[_ ]?company|carrier)\b", re.I), "payer_name"),
    (re.compile(r"\b(plan|plan[_ ]?name|benefit[_ ]?plan)\b", re.I), "plan_name"),
    (re.compile(r"\b(setting|care[_ ]?setting|place[_ ]?of[_ ]?service)\b", re.I), "setting"),
    (re.compile(r"\b(billing[_ ]?class|billing[_ ]?code)\b", re.I), "billing_class"),
]

_CHARGE_RULES: list[tuple[re.Pattern, re.Pattern, str]] = [
    (re.compile(r"\bgross\b", re.I), re.compile(r"\b(charge|price)\b", re.I), "gross_charge"),
    (re.compile(r"\bcash\b", re.I), re.compile(r"\b(price|charge|discount)\b", re.I), "discounted_cash"),
    (re.compile(r"\bnegotiated\b", re.I), re.compile(r"\b(dollar|rate|amount|price)\b", re.I), "negotiated_dollar"),
]


def _keyword_match(header: str, used_targets: set[str]) -> str | None:
    """Apply keyword heuristics to *header*.  Return internal name or None."""
    for pattern, _ in _SKIP_PATTERNS:
        if pattern.search(header):
            return None
    for primary, secondary, target in _CHARGE_RULES:
        if target not in used_targets and primary.search(header) and secondary.search(header):
            return target
    for pattern, target in _KEYWORD_RULES:
        if target not in used_targets and pattern.search(header):
            return target
    return None


def fuzzy_match_columns(headers: list[str], known_mapping: dict[str, str]) -> dict[str, str]:
    """Map raw CSV headers to internal column names via 3-pass algorithm.

    1. Exact match (case-insensitive, whitespace-stripped)
    2. Keyword heuristics
    3. difflib SequenceMatcher with ratio > 0.7
    """
    result: dict[str, str] = {}
    used_targets: set[str] = set()

    lower_mapping: dict[str, tuple[str, str]] = {
        k.strip().lower(): (k, v) for k, v in known_mapping.items()
    }

    remaining: list[str] = []

    # Pass 1 -- exact match
    for header in headers:
        key = header.strip().lower()
        if key in lower_mapping:
            _, target = lower_mapping[key]
            if target not in used_targets:
                result[header] = target
                used_targets.add(target)
        else:
            remaining.append(header)

    # Pass 2 -- keyword heuristics
    still_remaining: list[str] = []
    for header in remaining:
        matched = _keyword_match(header, used_targets)
        if matched is not None:
            result[header] = matched
            used_targets.add(matched)
        else:
            still_remaining.append(header)

    # Pass 3 -- difflib fuzzy
    for header in still_remaining:
        best_target: str | None = None
        best_ratio: float = 0.0
        stripped = header.strip().lower()
        for known_key, target in known_mapping.items():
            if target in used_targets:
                continue
            ratio = SequenceMatcher(None, stripped, known_key.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_target = target
        if best_ratio > 0.7 and best_target is not None:
            result[header] = best_target
            used_targets.add(best_target)

    return result


# ---------------------------------------------------------------------------
# CSV parsing helpers
# ---------------------------------------------------------------------------

def _parse_csv_line(line: str) -> list[str]:
    """Parse a single CSV line using csv.reader for proper quote handling."""
    try:
        return next(csv.reader([line]))
    except Exception:
        return [part.strip().strip('"') for part in line.split(",")]


def _looks_like_data_header(values: list[str]) -> bool:
    """Detect whether a row is a CMS-format tabular CSV header row."""
    normalized = {v.strip().lower() for v in values if v is not None}
    if not normalized:
        return False
    has_description = "description" in normalized
    has_code = any(k in normalized for k in ("code", "code_1", "code|1"))
    has_charge = any(
        k in normalized
        for k in ("standard_charge", "gross_charge", "standard_charge|gross")
    )
    has_billing = "billing_class" in normalized
    has_payer = "payer_name" in normalized
    return (has_description and has_code) or (has_charge and has_code) or (has_billing and has_payer)


def _find_data_header_row(file_path: Path, max_scan_rows: int = 50) -> int:
    """Scan first N rows to find the header row index."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for idx, line in enumerate(f):
                if idx >= max_scan_rows:
                    break
                stripped = line.strip()
                if not stripped:
                    continue
                values = _parse_csv_line(stripped)
                if _looks_like_data_header(values):
                    return idx
    except Exception:
        return 2
    return 2


def _extract_hospital_info(file_path: Path) -> dict[str, str]:
    """Extract hospital metadata from the first few rows.

    Returns dict with keys: name, location, address, last_updated, version.
    """
    fallback_name = (
        file_path.stem.replace("_standardcharges", "").replace("_", " ").title()
    )
    default = {"name": fallback_name, "location": "", "address": "", "last_updated": "", "version": ""}

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            raw_lines: list[str] = []
            for _ in range(6):
                line = f.readline()
                if not line:
                    break
                raw_lines.append(line.strip())

        parsed = [_parse_csv_line(line) for line in raw_lines if line]
        if not parsed:
            return default

        first = [v.strip() for v in parsed[0]]
        second = [v.strip() for v in parsed[1]] if len(parsed) > 1 else []
        third = [v.strip() for v in parsed[2]] if len(parsed) > 2 else []

        # Format A: row0="hospital_name", row1=<name>, row2=<meta fields>
        if first and first[0].lower() in ("hospital_name", "hospital name") and len(first) == 1:
            name = second[0] if second else fallback_name
            meta = third if len(third) >= 5 else []
            location = ", ".join([v for v in meta[:3] if v]) if meta else ""
            return {
                "name": name,
                "location": location,
                "address": meta[0] if meta else "",
                "last_updated": meta[3] if len(meta) > 3 else "",
                "version": meta[4] if len(meta) > 4 else "",
            }

        # Format B: row0="hospital_name,<name>,<location>,..."
        if first and first[0].lower() in ("hospital_name", "hospital name") and len(first) >= 2:
            return {
                "name": first[1] or fallback_name,
                "location": first[2] if len(first) > 2 else "",
                "address": first[3] if len(first) > 3 else "",
                "last_updated": first[4] if len(first) > 4 else "",
                "version": first[5] if len(first) > 5 else "",
            }

        # Format C (legacy): <name>,<last_updated>,<version>,<location>,<address>
        if len(first) >= 5 and not _looks_like_data_header(first):
            return {
                "name": first[0] or fallback_name,
                "last_updated": first[1],
                "version": first[2],
                "location": first[3],
                "address": first[4],
            }

        return default
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Wide-to-tall pivot for CMS standard charge CSVs
# ---------------------------------------------------------------------------

# Regex to detect payer-specific wide-format columns:
# standard_charge|Payer|Plan|negotiated_dollar  (or negotiated_percentage, methodology, etc.)
# estimated_amount|Payer|Plan
# additional_payer_notes|Payer|Plan
_WIDE_COL_RE = re.compile(
    r"^(standard_charge|estimated_amount|additional_payer_notes)\|(.+)\|"
    r"(negotiated_dollar|negotiated_percentage|negotiated_algorithm|methodology)$",
    re.IGNORECASE,
)

_WIDE_EST_RE = re.compile(
    r"^estimated_amount\|(.+)$",
    re.IGNORECASE,
)

_WIDE_NOTES_RE = re.compile(
    r"^additional_payer_notes\|(.+)$",
    re.IGNORECASE,
)


def _detect_wide_format_payers(headers: list[str]) -> dict[str, dict[str, str]]:
    """Detect payer/plan groups from wide-format column headers.

    Returns {payer_plan_key: {field_suffix: original_column_name, ...}}
    e.g. {"IBC|Indemnity": {"negotiated_dollar": "standard_charge|IBC|Indemnity|negotiated_dollar", ...}}
    """
    groups: dict[str, dict[str, str]] = {}

    for col in headers:
        m = _WIDE_COL_RE.match(col)
        if m:
            payer_plan, field = m.group(2), m.group(3)
            groups.setdefault(payer_plan, {})[field.lower()] = col
            continue

        # estimated_amount|Payer|Plan
        me = _WIDE_EST_RE.match(col)
        if me:
            payer_plan = me.group(1)
            groups.setdefault(payer_plan, {})["estimated_amount"] = col
            continue

        # additional_payer_notes|Payer|Plan
        mn = _WIDE_NOTES_RE.match(col)
        if mn:
            payer_plan = mn.group(1)
            groups.setdefault(payer_plan, {})["additional_notes"] = col

    return groups


def _split_payer_plan(payer_plan_key: str) -> tuple[str, str]:
    """Split a payer|plan key into (payer_name, plan_name).

    Uses the first segment as payer and the rest as plan.
    e.g. "IBC|PPO" -> ("IBC", "PPO")
         "Cigna|Commercial|Non|PPO" -> ("Cigna", "Commercial Non PPO")
    """
    parts = payer_plan_key.split("|")
    payer = parts[0].strip()
    plan = " ".join(p.strip() for p in parts[1:]) if len(parts) > 1 else ""
    return payer, plan


def _pivot_wide_to_tall(df: pl.DataFrame) -> pl.DataFrame:
    """Pivot a wide-format CMS CSV into tall (row-per-payer-plan) format.

    Detects payer-specific columns like standard_charge|Payer|Plan|negotiated_dollar,
    and creates one row per (procedure, payer, plan) combination.
    """
    payer_groups = _detect_wide_format_payers(df.columns)
    if not payer_groups:
        return df  # Not wide format, return as-is

    # Identify non-payer columns (shared across all payer rows)
    payer_cols_set: set[str] = set()
    for fields in payer_groups.values():
        payer_cols_set.update(fields.values())

    # Fields the pivot will generate -- exclude from shared to avoid duplication
    pivot_output_fields = {
        "payer_name", "plan_name", "negotiated_dollar", "negotiated_percentage",
        "negotiated_algorithm", "methodology", "estimated_amount", "additional_notes",
    }
    shared_cols = [
        c for c in df.columns
        if c not in payer_cols_set and c not in pivot_output_fields
    ]

    tall_frames: list[pl.DataFrame] = []

    for payer_plan_key, field_map in payer_groups.items():
        payer_name, plan_name = _split_payer_plan(payer_plan_key)

        # Skip groups that have no negotiated_dollar column
        if "negotiated_dollar" not in field_map:
            continue

        # Build select expressions: start with shared columns
        select_exprs: list[pl.Expr] = [pl.col(c) for c in shared_cols]
        select_exprs.append(pl.lit(payer_name).alias("payer_name"))
        select_exprs.append(pl.lit(plan_name).alias("plan_name"))

        for target_field in ("negotiated_dollar", "negotiated_percentage",
                             "negotiated_algorithm", "methodology",
                             "estimated_amount", "additional_notes"):
            if target_field in field_map:
                select_exprs.append(pl.col(field_map[target_field]).alias(target_field))
            else:
                select_exprs.append(pl.lit(None).alias(target_field))

        try:
            chunk = df.select(select_exprs)
            tall_frames.append(chunk)
        except Exception as e:
            logger.warning("Failed to pivot payer group %s: %s", payer_plan_key, e)
            continue

    if not tall_frames:
        return df

    result = pl.concat(tall_frames, how="diagonal_relaxed")
    return result


# ---------------------------------------------------------------------------
# CSV MRF parser (public)
# ---------------------------------------------------------------------------

def parse_csv_mrf(file_path: str | Path) -> pl.DataFrame:
    """Parse a CMS standard-charges CSV file into a normalized Polars DataFrame.

    Handles:
    - Header row detection (scans first 50 rows)
    - Wide-to-tall pivot for payer-specific columns (detected BEFORE rename)
    - CMS column mapping with fuzzy recognition
    - Numeric type casting
    """
    file_path = Path(file_path)

    header_row = _find_data_header_row(file_path)
    logger.info("Header row detected at line %d for %s", header_row, file_path.name)

    df = pl.read_csv(
        file_path,
        skip_rows=header_row,
        infer_schema_length=10000,
        ignore_errors=True,
        truncate_ragged_lines=True,
        null_values=["", "N/A", "NA", "null"],
    )

    # IMPORTANT: detect wide format BEFORE renaming columns.
    # CMS column mapping would rename the first payer group's columns
    # (e.g. standard_charge|IBC|Indemnity|negotiated_dollar -> negotiated_dollar)
    # which breaks wide-format detection for that group.
    df = _pivot_wide_to_tall(df)

    # Now rename CMS standard columns to internal names
    rename_mapping: dict[str, str] = {}
    for orig_col in df.columns:
        if orig_col in CMS_COLUMN_MAPPING:
            rename_mapping[orig_col] = CMS_COLUMN_MAPPING[orig_col]

    # Fuzzy fallback when fewer than 3 exact matches
    if len(rename_mapping) < 3:
        fuzzy_mapping = fuzzy_match_columns(df.columns, CMS_COLUMN_MAPPING)
        for orig, target in fuzzy_mapping.items():
            if orig == target:
                continue
            if orig not in rename_mapping and target not in rename_mapping.values():
                if target not in df.columns:
                    rename_mapping[orig] = target

    if rename_mapping:
        df = df.rename(rename_mapping)

    # Cast numeric columns
    numeric_cols = [
        "gross_charge", "discounted_cash", "negotiated_dollar",
        "negotiated_percentage", "estimated_amount", "min_charge", "max_charge",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    return df


# ---------------------------------------------------------------------------
# JSON MRF parser
# ---------------------------------------------------------------------------

def parse_json_mrf(file_path: str | Path) -> pl.DataFrame:
    """Parse a CMS JSON MRF file into a normalized Polars DataFrame.

    For files < 500 MB uses json.load(); larger files use ijson streaming.
    """
    file_path = Path(file_path)
    file_size = file_path.stat().st_size

    if file_size < 500 * 1024 * 1024:
        return _parse_json_small(file_path)
    else:
        return _parse_json_streaming(file_path)


def _parse_json_small(file_path: Path) -> pl.DataFrame:
    """Parse a JSON MRF file that fits in memory."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records: list[dict[str, Any]] = []

    # in_network section
    for item in data.get("in_network", []):
        billing_code = item.get("billing_code", "")
        billing_code_type = item.get("billing_code_type", "")
        description = item.get("description", "")

        for rate_group in item.get("negotiated_rates", []):
            for price in rate_group.get("negotiated_prices", []):
                records.append({
                    "description": description,
                    "code_1": billing_code,
                    "code_1_type": billing_code_type,
                    "payer_name": item.get("negotiation_arrangement", ""),
                    "plan_name": "",
                    "negotiated_dollar": price.get("negotiated_rate"),
                    "negotiated_percentage": None,
                    "negotiated_algorithm": price.get("negotiated_type"),
                    "methodology": None,
                    "billing_class": price.get("billing_class"),
                    "setting": None,
                    "gross_charge": None,
                    "discounted_cash": None,
                    "min_charge": None,
                    "max_charge": None,
                })

    # allowed_amounts section
    for item in data.get("allowed_amounts", []):
        billing_code = item.get("billing_code", "")
        billing_code_type = item.get("billing_code_type", "")
        description = item.get("description", "")

        for allowed_item in item.get("allowed_amounts", []):
            billing_class = allowed_item.get("billing_class")
            for payment in allowed_item.get("payments", []):
                records.append({
                    "description": description,
                    "code_1": billing_code,
                    "code_1_type": billing_code_type,
                    "payer_name": "Out-of-Network",
                    "plan_name": "",
                    "negotiated_dollar": payment.get("allowed_amount"),
                    "negotiated_percentage": None,
                    "negotiated_algorithm": "allowed_amount",
                    "methodology": None,
                    "billing_class": billing_class,
                    "setting": None,
                    "gross_charge": None,
                    "discounted_cash": None,
                    "min_charge": None,
                    "max_charge": None,
                })

    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records)


def _parse_json_streaming(file_path: Path) -> pl.DataFrame:
    """Parse a large JSON MRF file using ijson streaming."""
    import ijson

    records: list[dict[str, Any]] = []

    with open(file_path, "rb") as f:
        # Stream in_network items
        for item in ijson.items(f, "in_network.item"):
            billing_code = item.get("billing_code", "")
            billing_code_type = item.get("billing_code_type", "")
            description = item.get("description", "")

            for rate_group in item.get("negotiated_rates", []):
                for price in rate_group.get("negotiated_prices", []):
                    records.append({
                        "description": description,
                        "code_1": billing_code,
                        "code_1_type": billing_code_type,
                        "payer_name": item.get("negotiation_arrangement", ""),
                        "plan_name": "",
                        "negotiated_dollar": price.get("negotiated_rate"),
                        "negotiated_percentage": None,
                        "negotiated_algorithm": price.get("negotiated_type"),
                        "methodology": None,
                        "billing_class": price.get("billing_class"),
                        "setting": None,
                        "gross_charge": None,
                        "discounted_cash": None,
                        "min_charge": None,
                        "max_charge": None,
                    })

    # Second pass for allowed_amounts (ijson needs separate iteration)
    with open(file_path, "rb") as f:
        try:
            for item in ijson.items(f, "allowed_amounts.item"):
                billing_code = item.get("billing_code", "")
                billing_code_type = item.get("billing_code_type", "")
                description = item.get("description", "")

                for allowed_item in item.get("allowed_amounts", []):
                    billing_class = allowed_item.get("billing_class")
                    for payment in allowed_item.get("payments", []):
                        records.append({
                            "description": description,
                            "code_1": billing_code,
                            "code_1_type": billing_code_type,
                            "payer_name": "Out-of-Network",
                            "plan_name": "",
                            "negotiated_dollar": payment.get("allowed_amount"),
                            "negotiated_percentage": None,
                            "negotiated_algorithm": "allowed_amount",
                            "methodology": None,
                            "billing_class": billing_class,
                            "setting": None,
                            "gross_charge": None,
                            "discounted_cash": None,
                            "min_charge": None,
                            "max_charge": None,
                        })
        except ijson.JSONError:
            pass  # No allowed_amounts section

    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# Parquet normalization
# ---------------------------------------------------------------------------

CHARGES_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "description_id": pl.Int32,
    "code1": pl.Utf8,
    "code1_type": pl.Utf8,
    "setting": pl.Utf8,
    "billing_class": pl.Utf8,
    "gross_charge": pl.Float64,
    "discounted_cash": pl.Float64,
    "payer_id": pl.Int32,
    "plan_id": pl.Int32,
    "negotiated_dollar": pl.Float64,
    "negotiated_percentage": pl.Float64,
    "methodology": pl.Utf8,
    "min_charge": pl.Float64,
    "max_charge": pl.Float64,
}


def normalize_to_parquet(df: pl.DataFrame, hospital_name: str, output_dir: Path) -> dict[str, Any]:
    """Normalize a DataFrame into Parquet files with lookup tables.

    Writes:
    - charges.parquet   (main fact table with foreign keys)
    - descriptions.parquet
    - payers.parquet
    - plans.parquet
    - metadata.json

    Returns metadata dict.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Build lookup tables ---

    # Descriptions
    if "description" in df.columns:
        desc_unique = (
            df.select("description")
            .unique()
            .filter(pl.col("description").is_not_null())
            .sort("description")
        )
        desc_unique = desc_unique.with_row_index("description_id").with_columns(
            pl.col("description_id").cast(pl.Int32)
        )
        desc_unique = desc_unique.rename({"description": "text"})
    else:
        desc_unique = pl.DataFrame({"text": [], "description_id": []},
                                   schema={"text": pl.Utf8, "description_id": pl.Int32})

    # Payers
    if "payer_name" in df.columns:
        payer_unique = (
            df.select("payer_name")
            .unique()
            .filter(pl.col("payer_name").is_not_null())
            .sort("payer_name")
        )
        payer_unique = payer_unique.with_row_index("payer_id").with_columns(
            pl.col("payer_id").cast(pl.Int32)
        )
        payer_unique = payer_unique.rename({"payer_name": "name"})
    else:
        payer_unique = pl.DataFrame({"name": [], "payer_id": []},
                                    schema={"name": pl.Utf8, "payer_id": pl.Int32})

    # Plans
    if "plan_name" in df.columns:
        plan_unique = (
            df.select("plan_name")
            .unique()
            .filter(pl.col("plan_name").is_not_null())
            .sort("plan_name")
        )
        plan_unique = plan_unique.with_row_index("plan_id").with_columns(
            pl.col("plan_id").cast(pl.Int32)
        )
        plan_unique = plan_unique.rename({"plan_name": "name"})
    else:
        plan_unique = pl.DataFrame({"name": [], "plan_id": []},
                                   schema={"name": pl.Utf8, "plan_id": pl.Int32})

    # --- Build charges fact table ---
    charges = df.clone()

    # Join description IDs
    if "description" in charges.columns and len(desc_unique) > 0:
        charges = charges.join(
            desc_unique.rename({"text": "description"}),
            on="description",
            how="left",
        )
        charges = charges.drop("description")
    else:
        charges = charges.with_columns(pl.lit(None).cast(pl.Int32).alias("description_id"))

    # Join payer IDs
    if "payer_name" in charges.columns and len(payer_unique) > 0:
        charges = charges.join(
            payer_unique.rename({"name": "payer_name"}),
            on="payer_name",
            how="left",
        )
        charges = charges.drop("payer_name")
    else:
        charges = charges.with_columns(pl.lit(None).cast(pl.Int32).alias("payer_id"))

    # Join plan IDs
    if "plan_name" in charges.columns and len(plan_unique) > 0:
        charges = charges.join(
            plan_unique.rename({"name": "plan_name"}),
            on="plan_name",
            how="left",
        )
        charges = charges.drop("plan_name")
    else:
        charges = charges.with_columns(pl.lit(None).cast(pl.Int32).alias("plan_id"))

    # Rename code columns
    col_renames = {"code_1": "code1", "code_1_type": "code1_type",
                   "code_2": "code2", "code_2_type": "code2_type"}
    for old, new in col_renames.items():
        if old in charges.columns:
            charges = charges.rename({old: new})

    # Add row ID
    charges = charges.with_row_index("id").with_columns(pl.col("id").cast(pl.Int64))

    # Ensure all schema columns exist (add missing as null with correct type)
    for col_name, dtype in CHARGES_SCHEMA.items():
        if col_name not in charges.columns:
            charges = charges.with_columns(pl.lit(None).cast(dtype).alias(col_name))

    # Select only columns in CHARGES_SCHEMA, in schema order
    charges = charges.select(list(CHARGES_SCHEMA.keys()))

    # Cast to schema types
    for col_name, dtype in CHARGES_SCHEMA.items():
        try:
            charges = charges.with_columns(pl.col(col_name).cast(dtype, strict=False))
        except Exception:
            pass

    # --- Write Parquet files ---
    charges.write_parquet(output_dir / "charges.parquet", compression="zstd")
    desc_unique.write_parquet(output_dir / "descriptions.parquet", compression="zstd")
    payer_unique.write_parquet(output_dir / "payers.parquet", compression="zstd")
    plan_unique.write_parquet(output_dir / "plans.parquet", compression="zstd")

    metadata = {
        "hospital_name": hospital_name,
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(charges),
        "payer_count": len(payer_unique),
        "plan_count": len(plan_unique),
        "description_count": len(desc_unique),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    logger.info(
        "Normalized %d rows -> %s (%d payers, %d plans)",
        len(charges), output_dir, len(payer_unique), len(plan_unique),
    )
    return metadata


# ---------------------------------------------------------------------------
# Async download pipeline
# ---------------------------------------------------------------------------

async def download_mrf(url: str, hospital_id: str) -> Path:
    """Download an MRF file via httpx async streaming.

    Returns the path to the downloaded file.
    """
    import httpx as _httpx
    from shared.utils.http_client import get_client

    cache_dir = _hospital_cache_dir(hospital_id)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Determine filename from URL
    url_path = url.rsplit("/", 1)[-1].split("?")[0]
    if not url_path:
        url_path = "mrf_file"
    dest = cache_dir / url_path

    logger.info("Downloading MRF: %s -> %s", url, dest)

    # Streaming downloads use the pooled client directly
    client = get_client()
    async with client.stream(
        "GET", url,
        timeout=_httpx.Timeout(600.0, connect=30.0),
    ) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)

    logger.info("Downloaded %s (%.1f MB)", dest.name, dest.stat().st_size / 1024 / 1024)
    return dest


async def process_mrf(url: str, hospital_id: str, hospital_name: str = "") -> dict[str, Any]:
    """Full pipeline: check cache -> download -> parse -> normalize -> write Parquet.

    Returns metadata dict from normalize_to_parquet.
    """
    if is_cached(hospital_id):
        cache_dir = _hospital_cache_dir(hospital_id)
        meta = json.loads((cache_dir / "metadata.json").read_text(encoding="utf-8"))
        logger.info("Using cached data for %s (%d rows)", hospital_id, meta.get("row_count", 0))
        return meta

    # Download
    raw_file = await download_mrf(url, hospital_id)

    try:
        # Parse based on file extension
        suffix = raw_file.suffix.lower()
        if suffix == ".json":
            df = parse_json_mrf(raw_file)
        else:
            df = parse_csv_mrf(raw_file)

        if df.is_empty():
            raise ValueError(f"Parsed 0 records from {raw_file.name}")

        # Extract hospital name from file if not provided
        if not hospital_name and suffix != ".json":
            info = _extract_hospital_info(raw_file)
            hospital_name = info.get("name", hospital_id)

        # Normalize and write Parquet
        output_dir = _hospital_cache_dir(hospital_id)
        metadata = normalize_to_parquet(df, hospital_name or hospital_id, output_dir)

        return metadata
    finally:
        # Clean up raw file to save disk space
        try:
            if raw_file.exists() and raw_file.suffix.lower() in (".csv", ".json"):
                raw_file.unlink()
                logger.info("Cleaned up raw file: %s", raw_file.name)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# DuckDB query interface
# ---------------------------------------------------------------------------

def _query_parquet(hospital_id: str, sql: str, params: list | None = None) -> list[dict]:
    """Execute SQL against a hospital's cached Parquet files via DuckDB.

    Registers charges, descriptions, payers, plans as views.
    """
    cache_dir = _hospital_cache_dir(hospital_id)
    charges_path = cache_dir / "charges.parquet"
    if not charges_path.exists():
        raise FileNotFoundError(f"No cached data for hospital {hospital_id}")

        from shared.utils.duckdb_safe import safe_parquet_sql
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE VIEW charges AS SELECT * FROM {safe_parquet_sql(charges_path)}")

        desc_path = cache_dir / "descriptions.parquet"
        if desc_path.exists():
            con.execute(f"CREATE VIEW descriptions AS SELECT * FROM {safe_parquet_sql(desc_path)}")

        payer_path = cache_dir / "payers.parquet"
        if payer_path.exists():
            con.execute(f"CREATE VIEW payers AS SELECT * FROM {safe_parquet_sql(payer_path)}")

        plan_path = cache_dir / "plans.parquet"
        if plan_path.exists():
            con.execute(f"CREATE VIEW plans AS SELECT * FROM {safe_parquet_sql(plan_path)}")

        if params:
            result = con.execute(sql, params)
        else:
            result = con.execute(sql)

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        con.close()


def get_rates(
    hospital_id: str,
    cpt_codes: list[str],
    payer: str = "",
) -> list[dict]:
    """Query negotiated rates for CPT codes at a hospital.

    Joins charges with description, payer, and plan lookup tables.
    Optional payer filter (case-insensitive LIKE).
    """
    placeholders = ", ".join(["?" for _ in cpt_codes])
    params: list = list(cpt_codes)

    payer_clause = ""
    if payer:
        payer_clause = "AND LOWER(p.name) LIKE ?"
        params.append(f"%{payer.lower()}%")

    sql = f"""
        SELECT
            c.code1 AS cpt_code,
            COALESCE(d.text, '') AS description,
            COALESCE(p.name, '') AS payer_name,
            COALESCE(pl.name, '') AS plan_name,
            c.negotiated_dollar,
            c.negotiated_percentage,
            c.methodology,
            c.setting,
            c.billing_class,
            c.gross_charge,
            c.min_charge,
            c.max_charge
        FROM charges c
        LEFT JOIN descriptions d ON c.description_id = d.description_id
        LEFT JOIN payers p ON c.payer_id = p.payer_id
        LEFT JOIN plans pl ON c.plan_id = pl.plan_id
        WHERE c.code1 IN ({placeholders})
          AND c.negotiated_dollar IS NOT NULL
          {payer_clause}
        ORDER BY c.code1, c.negotiated_dollar DESC
    """
    return _query_parquet(hospital_id, sql, params)


def get_rate_stats(hospital_id: str, cpt_codes: list[str]) -> list[dict]:
    """Compute rate dispersion statistics per CPT code.

    Returns min, max, median, mean, q25, q75, stddev, plus derived iqr and cv.
    """
    placeholders = ", ".join(["?" for _ in cpt_codes])

    sql = f"""
        SELECT
            c.code1 AS cpt_code,
            COALESCE(d.text, '') AS description,
            COUNT(DISTINCT c.payer_id) AS payer_count,
            MIN(c.negotiated_dollar) AS min_rate,
            MAX(c.negotiated_dollar) AS max_rate,
            MEDIAN(c.negotiated_dollar) AS median_rate,
            AVG(c.negotiated_dollar) AS mean_rate,
            QUANTILE_CONT(c.negotiated_dollar, 0.25) AS q25,
            QUANTILE_CONT(c.negotiated_dollar, 0.75) AS q75,
            STDDEV(c.negotiated_dollar) AS std_dev
        FROM charges c
        LEFT JOIN descriptions d ON c.description_id = d.description_id
        WHERE c.code1 IN ({placeholders})
          AND c.negotiated_dollar IS NOT NULL
        GROUP BY c.code1, d.text
        ORDER BY c.code1
    """
    rows = _query_parquet(hospital_id, sql, list(cpt_codes))

    # Add derived fields
    for row in rows:
        q25 = row.get("q25")
        q75 = row.get("q75")
        mean = row.get("mean_rate")
        std = row.get("std_dev")

        row["iqr"] = (q75 - q25) if q25 is not None and q75 is not None else None
        row["cv"] = (std / mean) if std is not None and mean and mean != 0 else None

    return rows


def get_cache_metadata(hospital_id: str) -> dict:
    """Read cache metadata for a hospital. Returns empty dict if not cached."""
    d = _hospital_cache_dir(hospital_id)
    meta_path = d / "metadata.json"
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def get_all_cached_hospitals() -> list[dict]:
    """List all hospitals with valid cached Parquet data.

    Returns list of metadata dicts (hospital_name, hospital_id, row_count, etc.).
    """
    results: list[dict] = []
    if not _CACHE_DIR.exists():
        return results

    for d in sorted(_CACHE_DIR.iterdir()):
        if not d.is_dir():
            continue
        meta_path = d / "metadata.json"
        charges_path = d / "charges.parquet"
        if meta_path.exists() and charges_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["hospital_id"] = d.name
                results.append(meta)
            except Exception:
                continue

    return results


def get_cross_hospital_rates(cpt_codes: list[str]) -> list[dict]:
    """Query rates for CPT codes across ALL cached hospitals.

    Returns list of dicts with hospital_name, hospital_id, cpt_code, rates.
    """
    all_results: list[dict] = []

    for hospital_meta in get_all_cached_hospitals():
        hospital_id = hospital_meta["hospital_id"]
        hospital_name = hospital_meta.get("hospital_name", hospital_id)
        try:
            rates = get_rates(hospital_id, cpt_codes)
            for rate in rates:
                rate["hospital_name"] = hospital_name
                rate["hospital_id"] = hospital_id
                all_results.append(rate)
        except Exception as e:
            logger.warning("Failed to query %s: %s", hospital_id, e)

    return all_results
