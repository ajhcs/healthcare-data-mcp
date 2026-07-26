# Kaiser Permanente — primary-source perimeter dossier

- **Accessed:** 2026-07-25T20:10:00Z (UTC)
- **Research question:** What legal/entity perimeter can safely be registered for “Kaiser Permanente,” and where must a registry refuse a universal EIN or silent aggregation?
- **Source policy:** Primary official sources only: Kaiser Permanente / Permanente organizations, IRS filings and instructions, and CMS PECOS data. No IRS 990 Evidence Service, secondary nonprofit profiles, or commercial corporate databases were used.
- **Bottom line:** “Kaiser Permanente” is a coordinated health-care system and brand made up of separate organizations. The strongest primary sources support three operational pillars—health plans, hospitals, and regional Permanente Medical Groups—but not one universal legal entity or EIN. Even within a pillar, regional plans have distinct EINs and hospitals have facility-level CCNs/NPIs/DBAs. A safe registry must ask for the legal entity, function, geography, and measurement perimeter before assigning identifiers or aggregating values.

## 1. Adversarial perimeter conclusion

### Supported structure

1. **Kaiser Foundation Health Plan / regional Kaiser Foundation Health Plans** provide coverage.
2. **Kaiser Foundation Hospitals (KFH)** provides medical facilities and operates hospitals in several regions, but it is not the operator for every Kaiser-branded hospital enrollment.
3. **Eight regional Permanente Medical Groups (PMGs)** provide medical care. They are self-governed and physician-led; official Kaiser/Permanente sources describe them as independent and as partners/contractors, not as a single consolidated medical-group legal entity.

### Legal boundary, not just organizational branding

- Kaiser’s Northern California Permanente site expressly calls KFH, KFHP, and the PMGs **separate legal entities**, says KFH and KFHP share one board, and says each PMG is independent, self-governed, self-managed, and physician-led. Common governance of KFH/KFHP therefore does not collapse the three pillars into one legal person.
- KFHP’s 2024 Form 990 identifies KFH and five regional health plans with separate EINs. It also lists several PMGs as compensated **independent contractors** for medical services.
- Kaiser’s 2024 annual report reports financial results for KFHP, KFH, Risant Health, and their respective subsidiaries and affiliates. That reporting perimeter is broader than one filer/EIN and does not list the PMGs as if they were one consolidated taxpayer.
- IRS instructions state that each organization must have its own EIN and must not use another organization’s EIN even when the organizations are related. Therefore a “Kaiser Permanente EIN” field is ill-posed unless the requester first identifies the legal organization.

### Registry refusal rule

Do **not** assign an EIN to the bare name “Kaiser Permanente.” Require, at minimum:

- requested function: health plan/coverage, hospital/facility operator, physician medical group, or consolidated reporting system;
- geography/region;
- legal business name or official filing record;
- identifier type sought (EIN, NPI, CCN, PAC ID, state license, or brand/DBA);
- time period; and
- aggregation intent (single filer, controlled group, facilities operated by an entity, or published financial-report perimeter).

## 2. Entity and EIN facts directly supported by IRS records

The following mappings are from **KFHP Inc.’s tax-period 2024 Form 990**, filed in the IRS 2025 TEOS XML release. The IRS index identifies the filer record, and Schedule R supplies the related-entity names, EINs, legal domiciles, and controlling-entity fields.

| Legal organization in official filing | EIN | Legal domicile in Schedule R | Relationship field / safe interpretation | Confidence |
|---|---:|---|---|---|
| Kaiser Foundation Health Plan, Inc. | 94-1340523 | Filer address CA | Form 990 filer; its program description specifically says its Medicaid managed-care programs operated in California and Hawaii during 2024 | High |
| Kaiser Foundation Hospitals | 94-1105628 | CA | Separate related tax-exempt organization; Schedule R marks it controlled; separate EIN from KFHP | High |
| Kaiser Foundation Health Plan of Colorado | 84-0591617 | CO | Related tax-exempt organization; direct controlling entity “KFHP Inc”; controlled | High |
| Kaiser Foundation Health Plan of Georgia, Inc. | 58-1592076 | GA | Related tax-exempt organization; direct controlling entity “KFHP Inc”; controlled | High |
| Kaiser Foundation Health Plan of the Mid-Atlantic States, Inc. | 52-0954463 | MD | Related tax-exempt organization; direct controlling entity “KFHP Inc”; controlled | High |
| Kaiser Foundation Health Plan of the Northwest | 93-0798039 | OR | Related tax-exempt organization; direct controlling entity “KFHP Inc”; controlled | High |
| Kaiser Foundation Health Plan of Washington | 91-0511770 | WA | Related tax-exempt organization; direct controlling entity “KFHPW Hldgs”; controlled | High |

**Important boundary:** legal domicile is not a complete service-area statement. Likewise, a regional label is not permission to infer every subsidiary, facility, contract, or tax identifier in that geography.

### Contractual evidence for the medical-group boundary

KFHP Inc.’s Form 990 Part VII contractor table reports, for tax period 2024:

- The Permanente Medical Group — “MEDICAL SERVICES” — $16,218,406,066;
- Southern California Permanente Medical Group — “MEDICAL SERVICES” — $13,073,106,607; and
- Hawaii Permanente Medical Group — “MEDICAL SERVICES” — $365,655,869.

These entries are direct evidence that the filing health plan treated named PMGs as separate contractors for medical services. They do **not** expose PMG EINs, and the dossier intentionally does not infer those EINs from names, addresses, or third-party sources.

## 3. Eight regional Permanente Medical Groups

The official Permanente “Our medical groups” page lists these eight groups and describes them collectively as self-governed, physician-led, prepaid multispecialty medical groups that partner with Kaiser Foundation Health Plans and Hospitals:

1. Colorado Permanente Medical Group
2. Hawaii Permanente Medical Group
3. Mid-Atlantic Permanente Medical Group (Maryland, Virginia, and Washington, D.C.)
4. Northwest Permanente (Oregon and southern Washington)
5. The Permanente Medical Group (Northern California)
6. The Southeast Permanente Medical Group (Georgia)
7. Southern California Permanente Medical Group
8. Washington Permanente Medical Group

Official Permanente descriptions say the groups practice exclusively within Kaiser Permanente and partner with the plans and hospitals. The Northern California Permanente legal-structure disclosure is more explicit: each PMG is an independent legal entity and the three pillars have a mutually exclusive relationship. “Integrated,” “exclusive,” and “partner” describe coordination and contracting; they are not evidence that all PMGs have merged into KFHP or KFH.

**Unresolved here:** primary sources gathered in this pass do not provide an authoritative, current EIN for each PMG. A registry must keep each PMG unresolved at the EIN level until a state/employer/tax primary record for that exact legal entity and period is obtained.

## 4. Facility, operator, and identifier distinctions

### KFH facility perimeter in the 2024 Form 990

KFH’s tax-period 2024 Form 990 is filed under EIN **94-1105628**. Schedule H states that KFH:

- owns and operates **40 licensed hospitals**, including five licensed hospitals with multiple campuses, in California, Hawaii, and Oregon; and
- operates **3 hospitals on behalf of the Maui Health System**: Maui Memorial Medical Center, Kula Hospital, and Lanai Community Hospital.

This language requires separate `owner` and `operator` concepts. “Operates on behalf of” does not by itself establish KFH ownership of the Maui facilities.

Schedule H separately enumerates facilities with business/facility names, addresses, state license numbers, and facility numbers. Examples include Kaiser Foundation Hospital – Santa Clara, Kaiser Westside Medical Center, Kula Hospital, and Lanai Community Hospital. These facility rows sit inside one KFH Form 990; a facility name is not automatically a separate EIN-bearing organization.

### CMS PECOS: one legal organization, many facility enrollments—and an exception

CMS’s May 2026 Hospital Enrollments file defines `ORGANIZATION NAME` as the hospital’s legal business name and `DOING BUSINESS AS NAME` as its DBA. Examples:

| Legal business name | DBA / facility | CCN | NPI | PAC ID | What it proves |
|---|---|---:|---:|---:|---|
| Kaiser Foundation Hospitals | Kaiser Sunnyside Medical Center | 380091 | 1124182902 | 6800707456 | One KFH legal entity can have a distinct facility DBA, CCN, and NPI |
| Kaiser Foundation Hospitals | Kaiser Foundation Hospital – Santa Clara | 050071 | 1326119967 | 6800707456 | A second facility enrollment shares the KFH entity PAC ID but has different CCN/NPI |
| Kaiser Foundation Hospitals | Kaiser Foundation Hospital – Manteca | 050748 | 1740354851 | 6800707456 | Another separate facility identifier set under KFH |
| Kaiser Foundation Health Plan of Washington | Kaiser Permanente Central Hospital | 500052 | 1861522088 | 9032022579 | Not every Kaiser-branded hospital enrollment has KFH as legal business name/operator |

Thus an answer can be wrong in both directions:

- assigning KFH’s EIN to every Kaiser-named facility ignores facility identifiers and regional operator exceptions; and
- treating every DBA/CCN as a distinct tax organization invents legal entities not established by the source.

CMS also explains that a PAC ID links entity-level information and may be associated with multiple enrollment IDs. NPI, CCN, PAC ID, state license, DBA, and EIN must remain typed identifiers in the registry rather than interchangeable aliases.

## 5. Financial-report perimeter is not an EIN perimeter

Kaiser Permanente’s official 2024 annual report gives $115.8 billion in operating revenue and states that the financial results cover **Kaiser Foundation Health Plan, Inc., Kaiser Foundation Hospitals, Risant Health, and their respective subsidiaries and affiliates**. The related February 7, 2025 official release also calls the results consolidated and explains that 2024 included Risant Health and acquisitions of Geisinger and Cone Health.

Safe interpretation:

- the published financial total belongs to the expressly stated multi-organization reporting perimeter;
- it is not attributable solely to KFHP EIN 94-1340523, KFH EIN 94-1105628, or any PMG;
- “Kaiser Permanente” system metrics (revenue, members, facilities) can change perimeter over time, such as the 2024 inclusion of Risant Health; and
- comparison benchmarks must store the source period and perimeter statement, not only the brand label.

## 6. Primary-source evidence ledger

### A. Kaiser / Permanente official sources

#### A1. Northern California Permanente — “Our values”

- **URL:** https://northerncalifornia.permanente.org/our-values
- **Locator:** heading “Our Structure: Separate Organizations Committed to a Shared Mission.”
- **Source period:** current web disclosure; no visible effective date captured; accessed 2026-07-25.
- **Key evidence:** KFH and KFHP are each separate nonprofit companies; PMGs organize and deliver medical care; KFH and KFHP share one board; each PMG is independent/self-governed/self-managed/physician-led; KFH, KFHP, and PMGs are separate legal entities with a mutually exclusive relationship.
- **Confidence:** High for the organization’s own stated structure.
- **Limitations:** Descriptive governance disclosure, not articles of incorporation or a complete subsidiary register. Its statement that there are seven KP regions appears to reflect a particular framing/time and conflicts with current Permanente pages listing eight medical groups/regions; use the current eight-group page for enumeration.

#### A2. The Permanente Federation — “Our medical groups”

- **URL:** https://permanente.org/our-medical-groups/
- **Locator:** heading “What are Permanente Medical Groups?” and the eight-group list.
- **Source period:** current web disclosure; accessed 2026-07-25.
- **Key evidence:** eight named PMGs; self-governed and physician-led; practice exclusively within Kaiser Permanente; partner with Kaiser Foundation Health Plans and Hospitals; geographic labels for the groups.
- **Confidence:** High for current group names and the Federation’s relationship description.
- **Limitations:** Does not give formation jurisdiction, EIN, ownership cap table, or contract text for each group.

#### A3. The Permanente Federation — care model

- **URL:** https://permanente.org/about-us/our-care-model/
- **Locator:** heading “The Permanente Medicine care model, built for quality,” especially the bullets “Permanente Medical Groups provide the health care,” “Kaiser Foundation Health Plan provides the coverage,” and “Kaiser Foundation Hospitals provide the medical facilities”; also “Aligned mission, aligned goals.”
- **Source period:** current web disclosure; accessed 2026-07-25.
- **Key evidence:** functional allocation across the three pillars and exclusive coordination.
- **Confidence:** High for the organization’s model description.
- **Limitations:** Marketing/operating description; cannot substitute for legal filings or identify the precise entity in a transaction.

#### A4. Kaiser Permanente 2024 Annual Report

- **HTML URL:** https://about.kaiserpermanente.org/expertise-and-impact/annual-reports/2024-annual-report
- **PDF URL:** https://about.kaiserpermanente.org/content/dam/kp/mykp/documents/reports/2024_annual-report_ADA.pdf
- **Locator:** HTML heading “Financials”; PDF page 20, “FINANCIALS,” and its perimeter footnote.
- **Source period:** year ended 2024; accessed 2026-07-25.
- **Key evidence:** $115.8B operating revenue; financial results are for KFHP, KFH, Risant Health, and respective subsidiaries and affiliates.
- **Confidence:** High for the published reporting perimeter and headline figures.
- **Limitations:** Annual-report summary, not a legal-entity registry; inclusion of subsidiaries and affiliates is not itemized on this page.

#### A5. Kaiser Permanente 2024 financial-results release

- **URL:** https://about.kaiserpermanente.org/news/press-release-archive/kaiser-foundation-health-plan-hospitals-risant-health-report-2024-financial-results
- **Locator:** opening paragraph; sections “Membership,” “Capital spending,” and “KFHP/H & Risant Health annual financial summary.”
- **Source period:** year ended 2024; published 2025-02-07; accessed 2026-07-25.
- **Key evidence:** calls the figures consolidated; says 2024 onward includes Risant Health in year-end results, membership, and facility counts; explains the Geisinger and Cone Health acquisitions.
- **Confidence:** High for Kaiser’s stated reporting scope.
- **Limitations:** Press release, not audited footnotes; do not infer every legal affiliate from the consolidated label.

### B. IRS primary records

#### B1. IRS Form 990 downloads and 2025 index

- **Download portal URL:** https://www.irs.gov/charities-non-profits/form-990-series-downloads
- **Index URL:** https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv
- **Locator:** portal “Form 990 series (e-file) XML format” → 2025; index rows filtered by EIN/name.
- **Source period:** IRS 2025 publication index; accessed 2026-07-25.
- **Key index records:**
  - KFHP Inc., EIN 941340523, tax period 202412, object ID `202503219349309715`, archive `2025_TEOS_XML_11A`.
  - KFH, EIN 941105628, tax period 202412, object ID `202523219349309952`, archive `2025_TEOS_XML_11C`.
  - Separate 2024 Form 990 index rows also exist for the Colorado, Georgia, Mid-Atlantic, Northwest, and Washington health plans under the distinct EINs listed above.
- **Confidence:** High.
- **Limitations:** The index is a publication/filing locator, not substantive proof beyond its fields.

#### B2. KFHP Inc. 2024 Form 990 XML

- **Archive URL:** https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_11A.zip
- **ZIP member:** `202503219349309715_public.xml`
- **Locators:**
  - `Return/ReturnHeader/Filer` — legal filer and EIN.
  - `Return/ReturnData/IRS990/ProgSrvcAccomActy2Grp` — California and Hawaii program scope.
  - `Return/ReturnData/IRS990/ContractorCompensationGrp` — KFH and three named PMGs as medical-service contractors.
  - `Return/ReturnData/IRS990ScheduleR/IdRelatedTaxExemptOrgGrp` — related plans/KFH, EINs, legal domiciles, and controlling-entity fields.
- **Source period:** tax year ended 2024-12-31; IRS release archive 2025; accessed 2026-07-25.
- **Confidence:** High for filed names, EINs, amounts, and disclosed relationships.
- **Limitations:** Form 990 is filer-supplied tax disclosure; Schedule R terminology has tax-form definitions and is not a complete corporate genealogy. It does not provide PMG EINs.

#### B3. KFH 2024 Form 990 XML

- **Archive URL:** https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_11C.zip
- **ZIP member:** `202523219349309952_public.xml`
- **Locators:**
  - `Return/ReturnHeader/Filer` — KFH and EIN 941105628.
  - `Return/ReturnData/IRS990ScheduleH/HospitalFacilitiesGrp` — facility names, addresses, state license numbers, and facility numbers.
  - `Return/ReturnData/IRS990ScheduleH/SupplementalInformationDetail` with `FormAndLineReferenceDesc` “4 - Community information” — 40 owned-and-operated hospitals plus 3 operated on behalf of Maui Health System.
  - same element with “6 - Affiliated Health Care System” — three main Kaiser entities.
- **Source period:** tax year ended 2024-12-31; IRS release archive 2025; accessed 2026-07-25.
- **Confidence:** High.
- **Limitations:** Schedule H is a tax filing, not a current state-license or deed register. “Operates on behalf of” should not be converted to ownership.

#### B4. IRS Instructions for Form 990

- **URL:** https://www.irs.gov/instructions/i990
- **Locator:** “Specific Instructions” → Heading items → “Item D. EIN.”
- **Source period:** 2025 instructions; accessed 2026-07-25.
- **Key evidence:** each organization, including a subordinate, must have its own EIN; an organization should never use an EIN issued to another organization even when related.
- **Confidence:** High for EIN handling rule.
- **Limitations:** General filing instruction; it does not identify Kaiser entities.

### C. CMS primary records

#### C1. CMS Hospital Enrollments, May 2026

- **Landing page:** https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments
- **CSV URL:** https://data.cms.gov/sites/default/files/2026-05/4c668d34-e45a-4b9e-b5f7-dec7f1c333e1/Hospital_Enrollments_2026.05.01.csv
- **Data guidance:** https://data.cms.gov/sites/default/files/2026-02/Hospital_Data_Guidance.pdf
- **Locators:** CSV header plus rows keyed by CCNs `380091`, `050071`, `050748`, and `500052`; guidance page 10 definitions of PAC ID, `ORGANIZATION NAME`, and `DOING BUSINESS AS NAME`.
- **Source period:** latest data May 2026; accessed 2026-07-25.
- **Key evidence:** distinct legal-business-name and DBA fields; KFH PAC ID 6800707456 across multiple facility enrollments; distinct CCNs/NPIs; Washington Central Hospital enrollment under Kaiser Foundation Health Plan of Washington.
- **Confidence:** High for CMS PECOS enrollment fields at the stated snapshot.
- **Limitations:** PECOS enrollment data is not a complete ownership registry and the public file does not disclose EINs. Enrollment rows may represent main or other practice locations. Use the field semantics in the guidance.

## 7. Unresolved ambiguity and non-inferences

The following remain deliberately unresolved:

- no universal “Kaiser Permanente” legal name or EIN is supported;
- no PMG EIN is assigned from this evidence set;
- no assumption that “controlled” in Schedule R means the same relationship as ownership under every state-law or accounting definition;
- no assumption that the eight PMG regions map one-to-one to eight health-plan corporations;
- no assumption that legal domicile equals complete operating geography;
- no assumption that every Kaiser-branded hospital is operated by KFH (CMS shows a Washington exception);
- no assumption that KFH owns facilities it says it operates on another system’s behalf;
- no assumption that a facility’s DBA, CCN, NPI, PAC ID, state license, and EIN are interchangeable;
- no allocation of consolidated annual-report revenue to an individual EIN without entity-level statements; and
- no silent inclusion of Risant Health, Geisinger, Cone Health, or PMGs when a benchmark merely says “Kaiser Permanente.”

## 8. Candidate blind scope questions and gold answers

These are suitable for adversarial benchmark prompts. A gold response should prefer a short refusal plus the missing scope question over a plausible but unsupported identifier.

### Q1. “What is Kaiser Permanente’s EIN?”

**Gold:** There is no supported universal Kaiser Permanente EIN. Ask which legal entity and function. Examples: KFHP Inc. is 94-1340523; KFH is 94-1105628; regional plans have other EINs; PMGs are separate and unresolved here.

### Q2. “Kaiser reports consolidated results, so can I use 94-1340523 for all Kaiser Permanente revenue?”

**Gold:** No. The 2024 annual-report perimeter includes KFHP, KFH, Risant Health, and their subsidiaries/affiliates. Consolidated reporting does not make the result attributable to KFHP’s EIN alone.

### Q3. “KFH and KFHP share a board. Are they the same legal entity?”

**Gold:** No. Kaiser’s own legal-structure disclosure calls them separate legal entities, and the IRS filing assigns distinct EINs. Shared governance is not legal identity.

### Q4. “Are Permanente physicians employees of the national Kaiser health plan?”

**Gold:** Do not assume that. Official sources describe eight independent/self-governed physician-led PMGs that partner and work exclusively with Kaiser’s plans/hospitals; KFHP’s 990 lists named PMGs as medical-service contractors. Resolve the exact employer/group and region.

### Q5. “Which entity should represent Kaiser Permanente in Colorado?”

**Gold:** Ask what is being represented. Coverage/plan may point to Kaiser Foundation Health Plan of Colorado (EIN 84-0591617); physician care points to Colorado Permanente Medical Group; a facility or outside hospital relationship may be another entity/identifier. Do not collapse them.

### Q6. “Can I attach KFH EIN 94-1105628 to Kaiser Santa Clara Hospital?”

**Gold:** KFH is the CMS legal business name and the IRS filer for the facility set, so the entity link is supported, but preserve facility identifiers separately: Santa Clara has DBA, CCN 050071, NPI 1326119967, and KFH PAC ID 6800707456. Do not replace those with the EIN.

### Q7. “Every Kaiser hospital is owned and operated by KFH, correct?”

**Gold:** No. KFH says it owns and operates 40 licensed hospitals in CA/HI/OR and operates three Maui Health System hospitals on behalf of that system; CMS lists Kaiser Permanente Central Hospital under Kaiser Foundation Health Plan of Washington. Ask region and whether the question is ownership, operation, enrollment, or branding.

### Q8. “Should each Kaiser hospital facility get a separate EIN?”

**Gold:** Not from the facility name alone. KFH’s Schedule H lists many facilities within one KFH Form 990/EIN, while CMS gives facility-specific CCNs and NPIs. A separate EIN requires an authoritative legal-entity record, not a DBA or facility row.

### Q9. “Does Northwest Permanente share the Northwest health plan’s EIN?”

**Gold:** Do not infer that. The health plan’s EIN is 93-0798039. Northwest Permanente is separately described as an independent medical group; its EIN is not established in this dossier.

### Q10. “Can I compare 2023 and 2024 Kaiser member/facility counts without perimeter metadata?”

**Gold:** No. Kaiser states that 2024 onward year-end results, membership, and facility counts include Risant Health. Store source period and perimeter, or the comparison silently changes scope.

### Q11. “The entity is called ‘Kaiser Permanente of the Mid-Atlantic States.’ Which EIN?”

**Gold:** The supplied phrase is ambiguous and may be a brand/regional shorthand. For the health plan, the official IRS name is Kaiser Foundation Health Plan of the Mid-Atlantic States, Inc., EIN 52-0954463. Physician care points to Mid-Atlantic Permanente Medical Group. Ask which entity/function.

### Q12. “May I aggregate all eight PMGs into one tax filer because The Permanente Federation represents them?”

**Gold:** No. The Federation is a national leadership/consulting organization representing the groups’ shared interests; official sources still describe eight separate/self-governed PMGs. Representation and collaboration are not evidence of one tax filer.

## 9. Suggested registry shape

A robust perimeter registry should model:

- `brand_system`: Kaiser Permanente;
- `legal_entity`: exact official name, EIN when directly supported, domicile, effective period;
- `entity_role`: plan, hospital operator, medical group, federation, affiliate;
- `relationship`: controls, common_governance, contracts_with, partners_with, operates_on_behalf_of, affiliate_of;
- `region/service_area`: separately sourced from domicile;
- `facility`: DBA/name, address, state license;
- `provider_enrollment`: CCN, NPI, PAC ID, enrollment ID;
- `reporting_perimeter`: named included organizations/affiliates, period, consolidation basis; and
- `resolution_status`: verified, ambiguous, or refused_pending_scope.

The crucial invariant is: **a brand match or integrated-care relationship never authorizes copying an EIN or financial value across legal-entity boundaries.**
