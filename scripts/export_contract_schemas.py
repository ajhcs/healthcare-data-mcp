"""Regenerate checked-in JSON Schemas for public transport contracts."""

from __future__ import annotations

import json
from pathlib import Path

from shared.acquisition.scale_annual_discharges_packet import AnnualDischargesAcquisition
from shared.acquisition.scale_emergency_department_count_contract import (
    EmergencyDepartmentCountAcquisition,
)
from shared.acquisition.scale_essential_service_designation_count_contract import (
    EssentialServiceDesignationCountAcquisition,
)
from shared.acquisition.scale_physician_count_contract import PhysicianCountAcquisition
from shared.acquisition.scale_service_line_count_contract import ServiceLineCountAcquisition
from shared.acquisition.scale_safety_net_patient_mix_contract import SafetyNetPatientMixAcquisition
from shared.contracts.public_evidence import PublicEvidenceBundle

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    schemas = {
        ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json": PublicEvidenceBundle.model_json_schema(),
        ROOT
        / "contracts"
        / "v2"
        / "scale-tabular-input-family-acquisition.schema.json": AnnualDischargesAcquisition.model_json_schema(),
        ROOT
        / "contracts"
        / "v3"
        / "scale-physician-count-acquisition.schema.json": PhysicianCountAcquisition.model_json_schema(),
        ROOT
        / "contracts"
        / "v4"
        / "scale-service-line-count-acquisition.schema.json": ServiceLineCountAcquisition.model_json_schema(),
        ROOT
        / "contracts"
        / "v5"
        / "scale-safety-net-patient-mix-acquisition.schema.json": SafetyNetPatientMixAcquisition.model_json_schema(),
        ROOT
        / "contracts"
        / "v6"
        / "scale-emergency-department-count-acquisition.schema.json": EmergencyDepartmentCountAcquisition.model_json_schema(),
        ROOT
        / "contracts"
        / "v7"
        / "scale-essential-service-designation-count-acquisition.schema.json": EssentialServiceDesignationCountAcquisition.model_json_schema(),
    }
    for target, schema in schemas.items():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
