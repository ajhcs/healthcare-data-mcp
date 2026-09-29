# Expanded exact-filer Form 990 corpus research

Research checked 2026-07-24. This note proposes the next 16-filer,
36-return corpus for the known-EIN/known-period prototype. It resolves legal
filers; it is not a brand-name resolver and does not claim that one filer
consolidates every organization marketed under a health-system brand.

## Method

The filing record is taken only from the official IRS Tax Exempt Organization
Search (TEOS) e-file indexes:

- [IRS Form 990 series downloads](https://www.irs.gov/charities-non-profits/form-990-series-downloads)
- [2024 index](https://apps.irs.gov/pub/epostcard/990/xml/2024/index_2024.csv)
- [2025 index](https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv)
- [2026 index](https://apps.irs.gov/pub/epostcard/990/xml/2026/index_2026.csv)

For each EIN, the selection requires `RETURN_TYPE=990`, pins the exact
`OBJECT_ID`, and preserves the IRS `TAX_PERIOD`, download year, batch, DLN, and
nullable Return ID. “Tax year” below means the year in which the indexed tax
period ends; it is not the IRS download year. The XML source URL is
`https://apps.irs.gov/pub/epostcard/990/xml/{filing_year}/{XML_BATCH_ID}.zip`
and the member is `{OBJECT_ID}_public.xml`. The 2025 and 2026 May indexes collapse rows under `05A` although the official downloads are split across `05A` and `05B`; ingestion records the actual archive part that contains the member. Some part-B members use Deflate64, which is decoded with the host `unzip` utility before the same XML validation and hashing.

The 2026 index snapshot inspected on 2026-07-24 contains official tax-period
2025 Form 990 e-files for five selected June-year-end filers: Thomas Jefferson
University Hospitals, Rush University Medical Center, CommonSpirit Health,
CHRISTUS Health Plan, and Luminis Health. No 2025 return is selected
for the other eleven filers because this snapshot has no qualifying official
2025 e-file for them. Absence is snapshot-specific, not a claim that they will
never file a 2025 return.

The two most recent available periods are selected for every filer. A third
period is included for Thomas Jefferson University Hospitals, Rush University
Medical Center, CommonSpirit Health, and Luminis Health because their 2023
returns are already pinned in the compatible 2024 May batch. This produces 36
returns while staying within the requested two-to-three-period coverage.

## Required brand-name resolutions

| User shorthand | Exact legal filer selected | EIN | Scope boundary |
|---|---|---:|---|
| Houston Methodist | THE METHODIST HOSPITAL | 741180155 | Exact flagship/legal filer. Do not infer that this one return covers every Houston Methodist affiliate. |
| Jefferson Health | THOMAS JEFFERSON UNIVERSITY HOSPITALS INC | 232829095 | Exact hospital filer. Jefferson's current umbrella brand includes later combinations, including Lehigh Valley Health Network; those are not silently folded into this EIN. |
| MaineHealth | MaineHealth | 010238552 | Exact parent legal filer found under the matching legal name. |
| Rush | Rush University Medical Center | 362174823 | Exact academic medical-center filer, not automatic coverage of every Rush-branded entity. |

The current Jefferson brand site describes a 33-hospital network after its
2024 combination with Lehigh Valley Health Network, while the
[specific Thomas Jefferson University Hospitals page](https://www.jeffersonhealth.org/locations/thomas-jefferson-university-hospital/about-us)
describes the selected legal organization and its five primary service
locations. The corpus therefore uses the IRS filer identity, not the broader
current brand footprint.

## Proposed 16-filer set and diversity

The four existing filers remain: Providence St Joseph Health, Intermountain
Health Care, Kaiser Foundation Hospitals, and Mayo Clinic Group Return. The
twelve additions are the four required resolutions plus The Cleveland
Clinic Foundation, Mass General Brigham Incorporated & Affiliates Group
Return, CommonSpirit Health, Advocate Aurora Health, Texas Health Resources,
CHRISTUS Health Plan, Hennepin Healthcare System, and Luminis Health.

| Legal filer | EIN | Why it belongs in the test mix |
|---|---:|---|
| PROVIDENCE ST JOSEPH HEALTH | 811244422 | Existing large, multi-state nonprofit parent case. |
| INTERMOUNTAIN HEALTH CARE INC | 870269232 | Existing integrated nonprofit and combination-history case. |
| KAISER FOUNDATION HOSPITALS | 941105628 | Existing hospital legal entity inside an integrated payer-provider family; it must not be mislabeled as the health-plan filer. |
| MAYO CLINIC GROUP RETURN | 383952644 | Existing explicit group-return and academic case. |
| THE METHODIST HOSPITAL | 741180155 | Texas and academic case. [Houston Methodist reports eight acute-care hospitals](https://www.houstonmethodist.org/about-us/), but this corpus answers only for the selected EIN. |
| THOMAS JEFFERSON UNIVERSITY HOSPITALS INC | 232829095 | University-affiliated, merger-sensitive exact-entity case. |
| MaineHealth | 010238552 | Regional, straightforward matching-name case. [MaineHealth reports one Level 1 medical center and eight additional licensed hospitals](https://www.mainehealth.org/about-mainehealth). |
| Rush University Medical Center | 362174823 | University-affiliated exact medical-center case; Rush currently lists four hospital locations, but the selected return is one legal entity. |
| THE CLEVELAND CLINIC FOUNDATION | 340714585 | Straightforward academic nonprofit case. [Cleveland Clinic describes a 23-hospital global system](https://newsroom.clevelandclinic.org/2026/02/26/cleveland-clinic-ranked-no-3-hospital-in-the-world-by-newsweek), but the corpus remains scoped to the selected foundation EIN. |
| MASS GENERAL BRIGHAM INCORPORATED & AFFILIATES GROUP RETURN | 900656139 | Explicit group-return and Harvard-affiliated academic-system case; the [official member list](https://www.massgeneralbrigham.org/en/about/members-affiliations) provides a roughly medium-scale system example, but is not assumed to be identical to the group-return affiliate list. |
| COMMONSPIRIT HEALTH | 470617373 | Large merger case. [CommonSpirit says CHI and Dignity Health came together in 2019](https://www.commonspirit.org/news-articles/commonspirit-health-launches-as-new-health-system). |
| ADVOCATE AURORA HEALTH INC | 824184596 | Merger-sensitive nonprofit parent case. [Advocate says Advocate Aurora and Atrium combined in 2022 through a joint operating company while existing assets remained in their states](https://www.advocatehealth.org/news/advocate-aurora-health-and-atrium-health-complete-combination). |
| Texas Health Resources | 752702388 | Texas regional system. Its [official facts sheet reports 29 hospital locations, including unconsolidated joint ventures](https://www.texashealth.org/facts), so the brand count must not be treated as the filer’s consolidation scope. |
| CHRISTUS Health Plan | 452106295 | Deliberate Texas insurer/health-plan filer, not a hospital filer. [The plan offers Medicare Advantage, marketplace, employer, and military-family coverage](https://www.christushealthplan.org/shop-plans/about-us). |
| HENNEPIN HEALTHCARE SYSTEM INC | 421707837 | Public-system representation that actually has official Form 990 e-files. Its [audited statements](https://www.hennepinhealthcare.org/wp-content/uploads/2022/10/Hennepin-Healthcare-System_21-FS_Final.pdf) identify it as a public corporation, a Hennepin County component unit, and a section 501(c)(3) organization. |
| LUMINIS HEALTH INC | 521622253 | Small regional combination case. [Luminis says it was formed when Doctors Community Medical Center joined Anne Arundel Medical Center](https://www.luminishealth.org/en/about-us). |

This set provides two Texas systems, multiple academic systems, a true
health-plan filer, a public-corporation filer, explicit group and merger
cases, small two- to four-hospital regional examples, and roughly medium-scale
nine- to twelve-hospital system footprints alongside larger national systems.
Hospital counts are only diversity evidence from official system pages; they
are not facts extracted from, or coverage assertions about, the selected Form
990.

## Pinned official filing records

`Return ID` is retained as nullable because current IRS rows are often blank.
The tax period, not the shorthand year, is the query/provenance key.

| Legal filer | EIN | Tax year | Filing year | IRS tax period | Object ID | Index batch | DLN | Return ID |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| PROVIDENCE ST JOSEPH HEALTH | 811244422 | 2024 | 2025 | 202412 | 202523169349306307 | 2025_TEOS_XML_11C | 93493316063075 | — |
| PROVIDENCE ST JOSEPH HEALTH | 811244422 | 2023 | 2024 | 202312 | 202443189349303569 | 2024_TEOS_XML_11A | 93493318035694 | — |
| INTERMOUNTAIN HEALTH CARE INC | 870269232 | 2024 | 2025 | 202412 | 202523119349302662 | 2025_TEOS_XML_11C | 93493311026625 | — |
| INTERMOUNTAIN HEALTH CARE INC | 870269232 | 2023 | 2024 | 202312 | 202443109349303374 | 2024_TEOS_XML_11A | 93493310033744 | — |
| KAISER FOUNDATION HOSPITALS | 941105628 | 2024 | 2025 | 202412 | 202523219349309952 | 2025_TEOS_XML_11C | 93493321099525 | — |
| KAISER FOUNDATION HOSPITALS | 941105628 | 2023 | 2024 | 202312 | 202433209349302778 | 2024_TEOS_XML_11A | 93493320027784 | — |
| MAYO CLINIC GROUP RETURN | 383952644 | 2024 | 2025 | 202412 | 202523189349303887 | 2025_TEOS_XML_11C | 93493318038875 | — |
| MAYO CLINIC GROUP RETURN | 383952644 | 2023 | 2024 | 202312 | 202423139349303067 | 2024_TEOS_XML_11A | 93493313030674 | — |
| THE METHODIST HOSPITAL | 741180155 | 2024 | 2025 | 202412 | 202533119349301948 | 2025_TEOS_XML_11B | 93493311019485 | — |
| THE METHODIST HOSPITAL | 741180155 | 2023 | 2024 | 202312 | 202423189349310002 | 2024_TEOS_XML_11A | 93493318100024 | — |
| THOMAS JEFFERSON UNIVERSITY HOSPITALS INC | 232829095 | 2025 | 2026 | 202506 | 202641349349301439 | 2026_TEOS_XML_05A | 93493134014396 | — |
| THOMAS JEFFERSON UNIVERSITY HOSPITALS INC | 232829095 | 2024 | 2025 | 202406 | 202541349349308729 | 2025_TEOS_XML_05A | 93493134087295 | 23826779 |
| THOMAS JEFFERSON UNIVERSITY HOSPITALS INC | 232829095 | 2023 | 2024 | 202306 | 202411369349309571 | 2024_TEOS_XML_05a | 93493136095714 | 22588066 |
| MaineHealth | 010238552 | 2024 | 2025 | 202409 | 202542269349301509 | 2025_TEOS_XML_08A | 93493226015095 | — |
| MaineHealth | 010238552 | 2023 | 2024 | 202309 | 202412269349300861 | 2024_TEOS_XML_08A | 93493226008614 | 22940610 |
| Rush University Medical Center | 362174823 | 2025 | 2026 | 202506 | 202631349349312808 | 2026_TEOS_XML_05A | 93493134128086 | — |
| Rush University Medical Center | 362174823 | 2024 | 2025 | 202406 | 202501349349309015 | 2025_TEOS_XML_05A | 93493134090155 | 23452153 |
| Rush University Medical Center | 362174823 | 2023 | 2024 | 202306 | 202441349349305684 | 2024_TEOS_XML_05a | 93493134056844 | 22591459 |
| THE CLEVELAND CLINIC FOUNDATION | 340714585 | 2024 | 2025 | 202412 | 202513189349302136 | 2025_TEOS_XML_11D | 93493318021365 | — |
| THE CLEVELAND CLINIC FOUNDATION | 340714585 | 2023 | 2024 | 202312 | 202423199349303332 | 2024_TEOS_XML_11A | 93493319033324 | — |
| MASS GENERAL BRIGHAM INCORPORATED & AFFILIATES GROUP RETURN | 900656139 | 2024 | 2025 | 202409 | 202512269349302446 | 2025_TEOS_XML_08A | 93493226024465 | 23844036 |
| MASS GENERAL BRIGHAM INCORPORATED & AFFILIATES GROUP RETURN | 900656139 | 2023 | 2024 | 202309 | 202402269349300855 | 2024_TEOS_XML_08A | 93493226008554 | 22939121 |
| COMMONSPIRIT HEALTH | 470617373 | 2025 | 2026 | 202506 | 202641339349307439 | 2026_TEOS_XML_05A | 93493133074396 | — |
| COMMONSPIRIT HEALTH | 470617373 | 2024 | 2025 | 202406 | 202531349349305488 | 2025_TEOS_XML_05A | 93493134054885 | 23809922 |
| COMMONSPIRIT HEALTH | 470617373 | 2023 | 2024 | 202306 | 202441329349301254 | 2024_TEOS_XML_05a | 93493132012544 | 22586544 |
| ADVOCATE AURORA HEALTH INC | 824184596 | 2024 | 2025 | 202412 | 202503219349328340 | 2025_TEOS_XML_11A | 93493321283405 | — |
| ADVOCATE AURORA HEALTH INC | 824184596 | 2023 | 2024 | 202312 | 202443219349300224 | 2024_TEOS_XML_11A | 93493321002244 | — |
| Texas Health Resources | 752702388 | 2024 | 2025 | 202412 | 202543149349306069 | 2025_TEOS_XML_11B | 93493314060695 | — |
| Texas Health Resources | 752702388 | 2023 | 2024 | 202312 | 202403179349300125 | 2024_TEOS_XML_11A | 93493317001254 | — |
| CHRISTUS Health Plan | 452106295 | 2025 | 2026 | 202506 | 202641339349301529 | 2026_TEOS_XML_05A | 93493133015296 | — |
| CHRISTUS Health Plan | 452106295 | 2024 | 2026 | 202406 | 202611339349301531 | 2026_TEOS_XML_05A | 93493133015316 | — |
| HENNEPIN HEALTHCARE SYSTEM INC | 421707837 | 2024 | 2025 | 202412 | 202503089349302050 | 2025_TEOS_XML_11A | 93493308020505 | — |
| HENNEPIN HEALTHCARE SYSTEM INC | 421707837 | 2023 | 2024 | 202312 | 202413169349302056 | 2024_TEOS_XML_11A | 93493316020564 | — |
| LUMINIS HEALTH INC | 521622253 | 2025 | 2026 | 202506 | 202631349349312238 | 2026_TEOS_XML_05A | 93493134122386 | — |
| LUMINIS HEALTH INC | 521622253 | 2024 | 2025 | 202406 | 202511219349300841 | 2025_TEOS_XML_05A | 93493121008415 | 23429439 |
| LUMINIS HEALTH INC | 521622253 | 2023 | 2024 | 202306 | 202441309349303644 | 2024_TEOS_XML_05a | 93493130036444 | 22391916 |

## Scope and availability limits

- Current brand footprints can be materially broader than one legal filer,
  especially after a merger. The service must echo the legal filer and EIN
  from XML and must not answer brand-wide questions as though they were the
  same thing.
- The health-plan example is intentionally a plan filer. Its Form 990 facts
  are valid for that legal entity, but they are not hospital operating facts.
- Government hospitals are often legitimately absent from Form 990. The IRS
  explains that certain government hospital organizations are relieved from
  filing under Revenue Procedure 95-48
  ([IRS section 501(r) reporting](https://www.irs.gov/charities-non-profits/section-501r-reporting)).
  Hennepin is included only because its exact public corporation is also a
  section 501(c)(3) filer with official e-file XML. A public hospital district
  or state institution with no official 990 must be reported as outside this
  prototype, not substituted with a nearby foundation.
- IRS instructions also warn that a Form 990 cannot generally consolidate a
  different-EIN organization unless an allowed group-return or disregarded
  entity rule applies
  ([2025 Form 990 instructions](https://www.irs.gov/instructions/i990)).
  System-site hospital counts therefore remain classification evidence only.
- A nullable IRS Return ID is normal in these snapshots. DLN and Object ID
  must retain their actual names; neither is a “submission ID.”
- Before ingestion, each downloaded XML member still needs deterministic
  verification of header EIN, tax-period end, form type, object/member name,
  and SHA-256. The index table is discovery provenance, not a replacement for
  XML validation.
