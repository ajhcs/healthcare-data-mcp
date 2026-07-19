# Scale Emergency-Department Count Acquisition Mission Packet

Tracking bead: `HDM-3d9`

Classification: evidence acquisition and explicit public-source missingness

Downstream coordinator: `healthcare-toolkit-2rr9.6.3.11`

## Goal and authority boundary

Evaluate authoritative public sources for six comparable
`emergency_department_count` inputs. The desired input is the count of
distinct dedicated emergency departments under 42 CFR 489.24(b), including
qualifying on-campus and off-campus departments or facilities, within one
approved current product-system roster and common period.

Data MCP may freeze exact source bytes, identity rows, source semantics,
locators, receipts, rights, missingness, and open conflicts. It may not sum
facility flags, treat a CCN hospital as a department, infer campuses or current
membership, treat missing as No or zero, mix periods, aggregate facilities,
impute, calculate Scale, run sensitivity, project, adjudicate, recommend,
promote, write a profile, mutate production, or deploy.

No evaluated source reports the desired comparable count. All six cells are
`unavailable_public`; no numeric candidate or zero is reported. This is stage
1 of the sixth serial cycle. Its exact safety-net predecessor remains
`blocked` / `human_review_required` and is pinned to Data merge
`50eba1efda522e875ebfb0b3feadfd80f4073a78`, Toolkit binding merge
`9376d38758d2098b8c1da09aac615ea5d4affb50`, Agents review merge
`bd7b09545de1c3b7f17c306b6543440c493bc669`, Toolkit admission merge
`61a67481a9f8bb40e81a2f8f59061664ca5694ba`, and tracker merges carried in
the machine contract. The predecessor packet and reusable-manifest semantic
roots are respectively
`sha256:af7ac7ce87a991b227673cfa8b6d92374bd01625217e7e21835f39abb289f365`
and
`sha256:86f148e3627f4e2b655bb3bab1c0e225ae9a5ab25399e80e2411e3b1a04991c1`;
the exact-merge manifest transport is
`sha256:b00d79b155abe12bb24535f4b3b380c17483c415d974af257432f816ed2e268e`.

## Primary-source evaluation and custody

Six exact input artifacts are frozen:

- AHRQ 2023 system CSV: 106,647 bytes, payload
  `sha256:7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6`,
  exact 40-column header
  `sha256:9cc022051910c61c2f66e60b81a450985996cca7fa981c85bd38fa8a9853a79f`.
  It binds the six product identities but has no emergency-department field.
- AHRQ 2023 hospital linkage: 1,528,734 bytes, payload
  `sha256:a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727`.
  Its unit is a 2023 CCN hospital membership candidate, not a dedicated ED.
- CMS Hospital General Information modified 2026-04-28: 1,453,884 bytes,
  payload
  `sha256:83c98b2e8687580e0482b13e1e9acd5813534be243e5ccd9f55556a869595d40`.
  `Emergency Services` is one Yes/No facility flag, not a department or
  campus inventory.
- CMS metastore metadata for `xubh-q36u`: 1,215 bytes, payload
  `sha256:a421368204acb1b91b4074ef797145aac3a11be132ae285730577b151e370cc4`.
- CMS Hospital Data Dictionary, April 2026: 1,291,356 bytes and 105 pages,
  payload
  `sha256:cd5016abee26e914b273a8fea8ab698710ff60f1c53a1b66e43bbd7168f6cb81`.
  Physical page 20 declares Char(6) `Facility ID` and Char(3)
  `Emergency Services`, with no department-count or campus-enumeration field.
- eCFR 42 CFR 489.24 as of 2026-07-16: 32,721 bytes, payload
  `sha256:aa51da81ea3ffbee2da8dff522bcd7c64e9ba8c667acb608bdb8c08b61407546`.
  Paragraph (b) defines the required dedicated-ED unit using state licensing,
  public representation, or the one-third outpatient-visit test, but does not
  enumerate product-system departments.

The verifier checks the exact AHRQ linkage and HGI bytes and headers without
joining their rows. It does not count linked hospitals, sum `Emergency
Services` flags, calculate missing-row totals, or persist any per-system
candidate context. The sources remain separately evaluated unit evidence only.

The five newly frozen government artifacts are classified `public_domain`.
The inherited immutable AHRQ system artifact retains its earlier conservative
`unknown_review_required` classification; v6 does not rewrite predecessor
custody. Raw source bytes and local paths remain outside Git.

## Additive v6 contract and no-execution state

Contracts v1-v5 and their fixtures remain byte-immutable. Additive contract
`ushso.scale-emergency-department-count-acquisition.v6` validates exact source
bytes and semantics, all-six identity order, source-unit distinctions, exact
predecessor lineage, semantic self-hash, and the complete no-execution state.
Its Public Evidence Bundle v1 adapter preserves every prior-cycle commit,
artifact root, transport root, and terminal no-go while emitting six
artifacts, eleven receipts, six entities, zero observations, six
`unavailable_public` coverage rows, and six open definition/period/boundary
conflicts. It cannot promote missingness.

Pre-merge generated identities are:

- acquisition semantic:
  `sha256:e84905fd10e6a547689f737c2a10fdd38b7aaceb3a157f044e6bb056bea46b6a`;
- acquisition fixture bytes:
  `56f638d8fab0e0c769646a424f25bafb7107898f4ef7f7e8ec11e3440f3f5dd1`;
- evidence-input fixture bytes:
  `d1057779b813516f5e8df880c17f862ea0b32c75ef2cbea0052f9ad6f8b0a2bd`;
- evidence-input canonical:
  `sha256:a58f451457ed48f9a0dcf57ac8014444c5ffd64c9f9f605d5788ea16ae5fa94b`;
- public-bundle semantic self-hash:
  `sha256:009dd74070cc55a455578d1d34183bea0d6e79662a60c6ca61a9ead031bb6184`;
- generated schema bytes:
  `23bcb0cd88996d0ba46d571f7fc7b222efe706f4caa8e0a0d2dbfb9221146783`.

The checked-in evidence input retains a forty-zero producer placeholder. The
downstream owner must rebuild the merge commit from a clean export and bind its
exact producer SHA in temporary output; pre-merge hashes must not be relabeled
as merge outputs.

## Verification, privacy, runtime, and rollback

Tests reject source or lineage drift, roster narrowing, definition/period/unit
substitution, receipt loss, HGI flag summing, facility aggregation, campus or
membership inference, missing-as-No/zero, imputation, fabricated zero, numeric
candidate creation, closed conflicts, weakened no-go, fabricated authority,
and forbidden output injection. Exact custody verification reproduces the
AHRQ/HGI context and validates CMS metadata, dictionary page/markers, and eCFR
definition markers. Required final gates are focused and whole tests, Ruff,
targeted Pyright, schema parity, packaging and installed-tree checks, two clean
deterministic rebuilds, and independent Standards and mission reviews.

There is no route, migration, server registry, production configuration,
API/cache mutation, profile write, patient-level data, PHI, credential,
calculation, projection, promotion, or deployed behavior. Runtime impact is
limited to an additive repository CLI family dispatch and private contract
modules.

Rollback is a focused revert of the additive v6 modules, schema/fixtures, CLI
dispatch, tests, and this record. Do not rewrite v1-v5, delete upstream
evidence, alter raw custody, or repair Beads history.
