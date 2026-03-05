"""DRG-to-service-line mapping and case mix index computation.

Loads static bundled CSV files for DRG classification and IPPS weights.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent / "data"
_SL_MAP_CSV = _DATA_DIR / "drg_service_line_map.csv"
_WEIGHTS_CSV = _DATA_DIR / "drg_weights_fy2024.csv"

# In-memory caches (loaded once)
_sl_map: dict[str, str] | None = None
_weights: dict[str, float] | None = None


def _load_service_line_map() -> dict[str, str]:
    """Load DRG->service-line mapping from bundled CSV."""
    global _sl_map
    if _sl_map is not None:
        return _sl_map

    if not _SL_MAP_CSV.exists():
        logger.warning("DRG service line map not found: %s", _SL_MAP_CSV)
        _sl_map = {}
        return _sl_map

    try:
        df = pd.read_csv(_SL_MAP_CSV, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        _sl_map = dict(zip(df["drg_code"].str.strip().str.zfill(3), df["service_line"].str.strip()))
        logger.info("Loaded %d DRG->service-line mappings", len(_sl_map))
    except Exception as e:
        logger.warning("Failed to load service line map: %s", e)
        _sl_map = {}

    return _sl_map


def _load_drg_weights() -> dict[str, float]:
    """Load DRG relative weights from bundled CSV."""
    global _weights
    if _weights is not None:
        return _weights

    if not _WEIGHTS_CSV.exists():
        logger.warning("DRG weights file not found: %s", _WEIGHTS_CSV)
        _weights = {}
        return _weights

    try:
        df = pd.read_csv(_WEIGHTS_CSV, dtype={"drg_code": str}, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        _weights = {}
        for _, row in df.iterrows():
            code = str(row["drg_code"]).strip().zfill(3)
            try:
                _weights[code] = float(row["weight"])
            except (ValueError, KeyError):
                continue
        logger.info("Loaded %d DRG weights", len(_weights))
    except Exception as e:
        logger.warning("Failed to load DRG weights: %s", e)
        _weights = {}

    return _weights


def map_drg_to_service_line(drg_code: str) -> str:
    """Map a DRG code to its service line. Returns 'Other Medical' if unknown."""
    sl_map = _load_service_line_map()
    normalized = str(drg_code).strip().zfill(3)
    return sl_map.get(normalized, "Other Medical")


def get_drg_weight(drg_code: str) -> float:
    """Get the relative weight for a DRG code. Returns 1.0 if unknown."""
    weights = _load_drg_weights()
    normalized = str(drg_code).strip().zfill(3)
    return weights.get(normalized, 1.0)


def compute_cmi(drg_discharges: list[tuple[str, int]]) -> float:
    """Compute case mix index from list of (drg_code, discharge_count) tuples.

    CMI = sum(weight_i * discharges_i) / sum(discharges_i)
    """
    weights = _load_drg_weights()
    total_weighted = 0.0
    total_discharges = 0

    for drg_code, discharges in drg_discharges:
        normalized = str(drg_code).strip().zfill(3)
        weight = weights.get(normalized, 1.0)
        total_weighted += weight * discharges
        total_discharges += discharges

    if total_discharges == 0:
        return 0.0

    return round(total_weighted / total_discharges, 4)
