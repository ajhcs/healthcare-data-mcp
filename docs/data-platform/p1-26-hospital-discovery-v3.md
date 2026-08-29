# P1-26 hospital discovery v3

This lane owns a sealed, hospital-scoped discovery manifest. Each source URL
records URL and probe state; each evidence row records a candidate state. These
records are receipts and review inputs, not hospital ownership authority.

`owner_promotion_state` is intentionally `outstanding` and
`authority_state` is intentionally `non_authoritative` for every row. A later
hospital-data owner workflow must perform promotion after identity, scope, and
ownership review. Discovery code must not infer a system owner from a URL,
name, or candidate row.

The implementation is in `shared/acquisition/hospital_discovery.py` and uses
only absolute HTTP(S) URLs. Invalid URLs and failed probes remain explicit;
they are never silently removed or converted to missing facts.
