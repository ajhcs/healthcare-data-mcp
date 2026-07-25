# Baseline R2 source-research record

- Arm: `manual_style_source_research_baseline`
- Repetition: `2`
- Research window: `2026-07-24T20:41:10.072Z` to `2026-07-24T20:48:44.864Z`
- Allowed local inputs: `questions.snapshot.json`, `orders.snapshot.json`
- Local read invocations: `1`
- Web research invocations: `24`
- Method: manual-style web research using official organization and government primary sources only. Each web invocation counts once even when it contained multiple searches or open/click operations.

## J1 — Jefferson enterprise scope

Selected scope: Jefferson enterprise = Thomas Jefferson University + Jefferson Health + Jefferson Health Plans. TJUH is a hospital operator/group inside the clinical system, not a synonym for all Jefferson Health.

Sources:

- https://www.jefferson.edu/about.html
- https://www.jeffersonhealth.org/locations/thomas-jefferson-university-hospital/about-us

## P1 — Penn Medicine scope

Selected scope: Penn Medicine = UPHS + Perelman School of Medicine. It is neither UPHS alone nor the whole University of Pennsylvania.

Sources:

- https://www.pennmedicine.org/about
- https://web-standards.pennmedicine.org/content-guidelines/the-penn-medicine-style

## U1 — UPMC enterprise scope

Selected scope: controlled UPMC enterprise, including hospitals/Health Services and Insurance Services. Exclude the University of Pittsburgh, which is an academic affiliate rather than part of UPMC.

Sources:

- https://www.upmc.com/about/why-upmc
- https://www.upmc.com/about/facts

## J2 — Jefferson Health Plans EIN

Selected identifier: Health Partners Plans, Inc. (rebranded as Jefferson Health Plans), EIN `23-2379751`, for tax period 2023.

Caveat: Jefferson Health Plans is the brand; the filing entity remains the legal organization.

Sources:

- https://www.jeffersonhealth.org/content/dam/health2021/documents/financial/tjuh-financial-statements/tju-federal-ug-report-2023.pdf
- https://www.jeffersonhealthplans.com/home/about-us/news/health-partners-plans-rebrands-under-jefferson-health-plans/

## P2 — HUP CCN and perimeter

Selected identifier: CCN `390111`.

Caveat: Hospital of the University of Pennsylvania is a UPHS/Penn Medicine hospital component, not a separate enterprise.

Sources:

- https://data.cms.gov/tools/medicare-inpatient-hospital-look-up-tool/provider/390111
- https://www.pennmedicine.org/patient-resources/policies/financial-assistance/financial-assistance-policy

## U2 — UPMC Health Plan EIN

Selected identifier: UPMC Health Plan, Inc., EIN `25-1777713`.

Caveat: “UPMC Health Plan” is also a marketing umbrella for multiple licensed issuers; product-specific work must use the legal issuer printed on the policy or filing.

Sources:

- https://www.upmc.com/about/finances/irs-filings/filings
- https://www.upmchealthplan.com/marketplace/find_insurance.aspx

## J3 — LVHN dated membership

Selected result: not part of Jefferson on `2024-07-31`; the transaction closed and LVHN became part of Jefferson on `2024-08-01`.

Source:

- https://www.jefferson.edu/about/news-and-events/2024/08/jefferson-lehigh-valley-health-network-complete-combination.html

## P3 — Doylestown dated membership

Selected result: not part of UPHS on `2025-03-31`; official integration occurred on `2025-04-01`.

Source:

- https://www.pennmedicine.org/news/doylestown-health-joins-university-of-pennsylvania-health-system

## U3 — UPMC FY2023 parent filer

Selected identifier: UPMC parent organization, EIN `25-1423657`, for the fiscal year ended `2023-06-30`.

Caveat: select the parent return, not the separate UPMC Group return.

Source:

- https://www.upmc.com/about/finances/irs-filings/filings

## J4 — Thomas Jefferson University Hospital operator and facility

Selected identifiers: Thomas Jefferson University Hospitals, Inc. EIN `23-2829095`; Thomas Jefferson University Hospital CCN `390174`.

Caveat: EIN identifies the operator; CCN identifies the Medicare-certified facility.

Sources:

- https://www.jeffersonhealth.org/content/dam/health2021/documents/financial/tjuh-financial-statements/tju-federal-ug-report-2023.pdf
- https://www.cms.gov/medicare/medicare-general-information/medicareapprovedfacilitie/carotid-artery-stenting-facilities-items/thomas-jefferson-university-hospital-

## P4 — Penn Medicine EIN

Selected identifier after clarification: The Trustees of the University of Pennsylvania, EIN `23-1352685`, when the request means the core University legal entity used for Penn Medicine/Perelman School of Medicine purposes.

Caveat: Penn Medicine itself is an umbrella brand/governance structure; subsidiary or hospital-specific requests require the relevant legal entity.

Sources:

- https://www.med.upenn.edu/pennmedplannedgiving/charitable-bequest.html
- https://www.pennmedicine.org/about

## U4 — UPMC Presbyterian/Shadyside combined record

Selected identifiers: UPMC Presbyterian Shadyside EIN `25-0965480`; CCN `390164`.

Caveat: one combined licensed hospital record, but distinct Oakland/Presbyterian and Shadyside physical campuses.

Sources:

- https://www.upmc.com/careers/admin-fellowship/health-services-admin/presbyterian-shadyside
- https://www.cms.gov/files/document/2024-reporting-cycle-teaching-hospital-list-oct2023p.pdf
