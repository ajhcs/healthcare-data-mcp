# Penn/UPHS and UPMC benchmark primary-source dossier

Research date: 2026-07-24

## Boundary and use

This note supports a deliberately small Health-System Perimeter Registry benchmark. It is not an exhaustive entity census, does not establish additive financial totals, and does not make a brand name stand in for a legal filer. The proposed records use the existing registry vocabulary: `systems`, `entities`, `facilities`, `reporting_perimeters`, `relationships`, `evidence`, and `gaps`.

The evidence set is limited to sources owned by the relevant university or health system, official audited financial statements, official filings hosted by the IRS or the filer, and government sources. A source's date limits every fact derived from it. In particular, an older Form 990 can validate the EIN printed on that filing but cannot, by itself, prove a complete current organizational perimeter.

## Penn Medicine / University of Pennsylvania Health System

### Minimal facts suitable for fixture records

| Proposed record | Fact and scope rule | Effective/source period | Confidence | Owning source |
| --- | --- | --- | --- | --- |
| `system:penn_medicine` | `Penn Medicine` is an umbrella for two constituents: the Perelman School of Medicine (PSOM) and the University of Pennsylvania Health System (UPHS). It is a governance/enterprise label, not a silently inferred EIN. | Governance structure established in 2001; current page accessed 2026-07-24 | High | P1, P2 |
| `system:uphs` | UPHS is the clinical delivery system. Its audited combined perimeter includes named operating entities and is narrower than the whole University and narrower than the Penn Medicine umbrella because PSOM is the other Penn Medicine constituent. Do not assign the UPHS label one umbrella EIN. | FY ended 2023-06-30 | High for the FY2023 perimeter | P2, P3 |
| `entity:trustees_upenn` | Legal filer: **Trustees of the University of Pennsylvania**, EIN **23-1352685**. The University Board of Trustees retains formal institutional governance and fiduciary responsibility for Penn Medicine, including UPHS. This does not make every UPHS affiliate a division of this filer. | EIN confirmed in IRS return for tax period ended 2022-06 and in an official 2024 employee-plan notice; governance page current when accessed | High | P1, P4, P5 |
| `component:psom` | PSOM is the University medical school and one of the two Penn Medicine constituents. Do not treat it as synonymous with UPHS or assign a separate EIN from this dossier. | Current page accessed 2026-07-24 | High | P1, P2 |
| `facility:hup` | **Hospital of the University of Pennsylvania (HUP)** is a named UPHS operating component and an operating division of the Trustees in the cited IRS Schedule H description. For an exact-filer question limited to this evidence, HUP maps to the Trustees filer/EIN **23-1352685**; label the operating-division basis. | IRS tax period ended 2022-06; corroborated by FY2023 UPHS audit | High | P3, P4 |
| `entity_or_facility:pennsylvania_hospital` | **Pennsylvania Hospital of the University of Pennsylvania Health System** is distinct from HUP and is separately named in the UPHS combined statements. Its IRS Form 990 for the tax year ended 2018-06 identifies EIN **31-1538725**. Do not collapse it into HUP. | EIN evidence is dated 2018-06; entity remains separately named in FY2023 audit | High for distinction; medium for using the old EIN as a current answer without revalidation | P3, P6 |
| `facility:doylestown_hospital` / `relationship:joined_uphs` | Doylestown Health officially joined UPHS on **2025-04-01** and became Penn Medicine Doylestown Health; Doylestown Hospital became the seventh Penn Medicine hospital. A 2024-dated question must not include it merely because integration plans existed. | Effective 2025-04-01 | High | P7 |
| `external:chop` | Children's Hospital of Philadelphia (CHOP) is a close academic/research collaborator but falls outside the Penn Medicine covered entity in the official Penn clinical-research guidance. A natural “Penn” question that could mean an academic affiliation may accept a caveated answer, but CHOP must not be silently aggregated into UPHS/Penn Medicine. | Page current when accessed 2026-07-24 | High for covered-entity exclusion; scope-dependent for other meanings of “affiliated” | P8 |

### Penn reporting-perimeter rules

1. **Penn Medicine governance umbrella:** PSOM + UPHS. Use for natural brand questions only when the response labels both constituents. It is not an exact-EIN perimeter. Source: P1/P2.
2. **UPHS FY2023 audited combined perimeter:** the audit names CPUP, CCA, HUP, Penn Presbyterian Medical Center, Pennsylvania Hospital, Chester County Hospital and Health System, Lancaster General Health, Princeton Healthcare System, Wissahickon Hospice/Homecare, and risk-retention/captive activity. It also says UPHS is included in the Trustees' financial statements. This list is bounded to FY2023 and predates Doylestown's 2025 integration. Source: P3, Note 1, page 8.
3. **University legal/consolidated perimeter:** the University includes academic activities and UPHS activity; it is broader than delivery-only. Do not use University-wide results for a question asking only for hospital or health-system delivery. Sources: P3, P9.
4. **Facility/filer perimeter:** HUP and Pennsylvania Hospital are separate facilities/operating entities even though both are in UPHS. An exact-EIN request must resolve which one the user means. Sources: P3, P4, P6.

### Penn unresolved ambiguity to preserve

- `Penn Medicine`, `UPHS`, `the University`, `HUP`, and `Pennsylvania Hospital` are not interchangeable.
- UPHS's FY2023 audit is not a current exhaustive census after Doylestown joined in 2025.
- This dossier does not establish a current consumer health-plan legal entity within Penn Medicine. The 1993 formation announcement described a planned “managed care entity,” but that historical statement is not current perimeter evidence and should not be converted into a current plan node.
- The Pennsylvania Hospital EIN is verified from an IRS-hosted return for a tax period ending in 2018. Revalidate it in a newer official filing before treating it as current production data.
- CHOP is outside the Penn Medicine covered entity, while academic appointments and collaborations can still make “affiliated with Penn” true in a different sense. Accepted benchmark answers must distinguish those meanings.
- No exhaustive subsidiary, facility, obligated-group, payer, or statutory-insurer inventory was attempted.

### Penn source ledger

**P1 — Penn Medicine Board, University Secretary.**
URL: https://secretary.upenn.edu/trustees-governance/penn-medicine-board
Locator: paragraph beginning “In 2001”; it says the integrated structure oversees PSOM and UPHS, calls the umbrella “Penn Medicine,” and says the prior UPHS trustee board was dissolved. The next governance paragraph says formal institutional governance and fiduciary responsibility, including UPHS, rests with the University Board of Trustees.
Publication/effective date: no page publication date; governing resolution occurred in 2001; accessed 2026-07-24.
Confidence: high.
Limitation: governance evidence, not a legal-entity census or EIN source.

**P2 — About the Dean, Perelman School of Medicine.**
URL: https://www.med.upenn.edu/evpdean/about-the-dean.html
Locator: first biographical paragraph under “Jonathan A. Epstein, M.D.”; exact locator phrase “Together these two entities make up Penn Medicine,” referring to PSOM and UPHS. The following paragraph describes UPHS's clinical-network scope.
Publication/effective date: current page, no publication date; accessed 2026-07-24.
Confidence: high.
Limitation: brand/component definition, not legal ownership or filing perimeter.

**P3 — University of Pennsylvania Health System FY2023 combined financial statements.**
URL: https://www.finance.upenn.edu/wp-content/uploads/UPHS-FY23-Financial-Statements.pdf
Locator: independent auditor report, pages 2–3, dated 2023-09-28; Note 1 “Organization,” page 8, especially the operating-entity list and the statements that PSOM and UPHS operate under Penn Medicine governance and that UPHS is included in the Trustees' financial statements; supplementary combining information, pages 44–45, with separate columns for HUP and PAH-UPHS.
Publication/effective date: years ended 2023-06-30 and 2022-06-30; auditor report 2023-09-28.
Confidence: high.
Limitation: FY2023 perimeter; predates Doylestown's 2025 integration and is not a legal-entity/EIN crosswalk for every column.

**P4 — Trustees of the University of Pennsylvania IRS Form 990.**
URL: https://apps.irs.gov/pub/epostcard/cor/231352685_202206_990_2023061221444523.pdf
Locator: Form 990 header (legal name and EIN **23-1352685**); Schedule H, Part VI, line 6 supplemental affiliated-healthcare-system description, which identifies HUP as an operating division within the system.
Publication/effective date: Form 990 for tax period ending 2022-06 (2021 form); IRS-hosted filing.
Confidence: high.
Limitation: filing-specific and not a current exhaustive UPHS inventory.

**P5 — University of Pennsylvania Matching Plan summary annual report.**
URL: https://www.hr.upenn.edu/docs/default-source/benefits/sar-pdfs/summary-annual-report-for-the-university-of-pennsylvania-matching-plan.pdf?sfvrsn=dd169456_15
Locator: opening paragraph, “EIN 23-1352685, Plan No. 001,” and later contact reference to Trustees of the University of Pennsylvania.
Publication/effective date: plan year 2024-01-01 through 2024-12-31.
Confidence: high for the Trustees EIN.
Limitation: employee-plan notice; it corroborates filer identity but does not define the health-system perimeter.

**P6 — Pennsylvania Hospital IRS Form 990.**
URL: https://apps.irs.gov/pub/epostcard/cor/311538725_201806_990_2019120716933827.pdf
Locator: Form 990 header: “Pennsylvania Hospital of the University of Pennsylvania Health System,” EIN **31-1538725**; Part III program-service description identifies Pennsylvania Hospital as the hospital described by the filing.
Publication/effective date: tax year ended 2018-06 (2017 form).
Confidence: high for the dated filing; medium for present-day use until a newer official filing confirms it.
Limitation: older than the benchmark date.

**P7 — Doylestown Health joins UPHS, Penn Medicine.**
URL: https://www.pennmedicine.org/news/doylestown-health-joins-university-of-pennsylvania-health-system
Locator: release date and first three paragraphs; exact locator phrase “officially joined ... today”; later paragraph stating Doylestown Hospital became the seventh Penn Medicine hospital and listing prior join years for Chester County, Lancaster General, and Princeton.
Publication/effective date: 2025-04-01; integration effective that day.
Confidence: high.
Limitation: official transaction announcement, not an EIN or complete subsidiary inventory.

**P8 — PennChart access guidance, Penn Medicine Clinical Research.**
URL: https://www.med.upenn.edu/clinicalresearch/pennchart.html
Locator: “Access for Non-Penn Users,” lines/paragraphs defining the Penn Medicine covered entity as UPHS + PSOM and identifying all other UPenn entities and CHOP as third parties outside it.
Publication/effective date: no publication date; accessed 2026-07-24.
Confidence: high for this covered-entity perimeter.
Limitation: HIPAA/clinical-research perimeter; it does not negate separate academic affiliations.

**P9 — University Finance, Health System Interfund.**
URL: https://www.finance.upenn.edu/accounting-reporting/health-system-interfund/
Locator: “Overview” says UPHS statements are consolidated monthly into University statements; “CHOP Health Affiliates” describes contracted CHOP practices separately.
Publication/effective date: current finance guidance; accessed 2026-07-24.
Confidence: high.
Limitation: accounting-process guidance, not audited entity ownership evidence.

**P10 — Official 1993 UPHS formation notice (historical only).**
URL: https://almanac.upenn.edu/archive/v40pdf/n01/071393.pdf
Locator: page 2 of the PDF (printed page 2), “PennMed is now the University Health System”; states the official UPHS date as 1993-07-01 and describes then-planned components, including a managed-care entity.
Publication/effective date: published 1993-07-13; UPHS effective 1993-07-01.
Confidence: high as historical evidence.
Limitation: not evidence of the current system or a current health-plan entity.

## UPMC

### Minimal facts suitable for fixture records

| Proposed record | Fact and scope rule | Effective/source period | Confidence | Owning source |
| --- | --- | --- | --- | --- |
| `system:upmc_brand` | Natural `UPMC` questions can refer to an integrated provider-and-insurer enterprise. The audited consolidation includes UPMC and subsidiaries, spanning nonprofit and for-profit entities and health-insurance products. Never reduce this brand to a hospital-only perimeter without saying so. | Year ended 2025-12-31 | High | U1 |
| `entity:upmc_parent` | Legal parent filer **UPMC**, EIN **25-1423657** in the official FY2022 filing. UPMC's current filings page distinguishes the parent return from the group return. | EIN source period ended 2022-06; separate FY2025 parent filing linked by UPMC | High for dated EIN; current header revalidation required | U2, U3 |
| `reporting_perimeter:upmc_group_990` | **UPMC Group**, EIN **20-8295721**, is the consolidated/group Form 990 reporting vehicle, not the same identifier as the UPMC parent. Do not answer “UPMC EIN” without clarifying parent versus group return versus facility/plan. | IRS group filing evidence; UPMC links FY2025 group return separately | High | U2, U3 |
| `external:university_of_pittsburgh` | The University of Pittsburgh, EIN **25-0965591**, is closely affiliated with UPMC but separately incorporated and excluded from UPMC consolidation; the University may designate one-third of UPMC board votes. | Relationship confirmed in Pitt FY2023 and UPMC 2025 statements; EIN current on Pitt page | High | U1, U4, U5 |
| `component:upmc_insurance_services` | UPMC's integrated enterprise includes insurance services. This is a component/reporting concept, not one assumed EIN. | Year ended 2025-12-31/current facts page | High | U1, U6 |
| `brand:upmc_health_plan` | “UPMC Health Plan” is a current marketing name for seven listed companies, not one safe EIN alias. **UPMC Health Plan, Inc.** is one specific health-insurance corporation, EIN **23-2813536** in dated IRS evidence; an exact-EIN question must identify the product/carrier and source period. | Marketing disclosure current 2026-07-24; EIN confirmed for tax period ended 2022-06 | High for multi-company brand; high for dated EIN | U6, U7 |
| `facility:upmc_presbyterian` and `facility:upmc_shadyside` | UPMC Presbyterian and UPMC Shadyside are distinct hospital campuses/facilities. Public records may combine them as “UPMC Presbyterian Shadyside.” Dated FY2022 evidence associates that combined organization with EIN **25-0965480**, while a current CMS record uses CCN **390164**. Preserve the sites and identifier types; do not claim current EIN continuity without revalidation. | Facility pages current 2026-07-24; EIN source period ended 2022-06; CMS re-certification 2024-08-14 | High for facility distinction and dated identifiers | U8, U9 |
| `relationship:washington_joined_upmc` | Washington Health System joined UPMC effective **2024-06-01**; UPMC's audited statements treat the transaction as an affiliation/acquisition and recognize the acquiree's net assets in consolidation. A pre-2024-06-01 dated question must not include it. | Effective 2024-06-01 | High | U1, U10 |

### UPMC reporting-perimeter rules

1. **UPMC audited enterprise:** UPMC and subsidiaries, with intercompany eliminations, covering delivery, financing/health plans, and other services. The University of Pittsburgh is a related academic partner, not part of the consolidation. Source: U1.
2. **UPMC parent Form 990:** exact legal parent filer, EIN 25-1423657. It is not interchangeable with the group return. Sources: U2/U3.
3. **UPMC Group Form 990:** group/consolidated IRS return, EIN 20-8295721. It is a tax-reporting perimeter, not a license to add every branded or affiliated organization. Sources: U2/U3.
4. **Insurance component:** consolidated “Health Plans” and UPMC Insurance Services are broader than UPMC Health Plan, Inc. An exact plan-EIN question requires product/carrier disambiguation. Sources: U1/U6/U7.
5. **Facility/filer perimeter:** Presbyterian and Shadyside remain distinct facilities even where one filing or ranking label combines them. Sources: U8/U9.

### UPMC unresolved ambiguity to preserve

- `UPMC` can mean the parent corporation, audited consolidated enterprise, IRS group return, provider network, or brand. These perimeters have different identifiers and uses.
- `UPMC Health Plan` is not a safe one-EIN alias. The official marketplace disclosure names seven companies behind the marketing name; the fixture should include only specifically validated nodes and leave the rest un-inventoried.
- UPMC and the University of Pittsburgh are legally separate but have shared academic objectives, contractual support, dual appointments, and University-appointed UPMC directors. “Affiliated” is true; “same entity” and silent financial aggregation are false.
- A combined UPMC Presbyterian Shadyside filer/reporting label does not erase the two campuses/facilities.
- Washington Health System's 2024 integration is well evidenced, but no exhaustive post-integration legal-entity inventory was attempted.
- UPMC's official FY2025 Form 990 PDFs are image-based. The official index clearly distinguishes parent and group returns; this dossier anchors exact parent/group EINs to UPMC-hosted FY2022/FY2017 filings and labels those source periods. Revalidate the FY2025 form headers before production use.

### UPMC source ledger

**U1 — UPMC year-end 2025 audited consolidated financial statements.**
URL: https://dam.upmc.com/-/media/upmc/about/finances/documents/cy2025-q4-financial-and-operating-report.pdf?hash=B615D7715EA6082DB4D3CDF3D8D2C931&rev=db24df59c26a488cbce3a153c84e9637
Locator: MD&A, PDF page 2; “Condensed Consolidating Statements of Operations,” PDF page 7, showing Health Services, Insurance Services, Eliminations, and Consolidated; divisional definitions, PDF page 8; independent auditor report, PDF page 21; Note 1, PDF page 26, describing UPMC and subsidiaries, nonprofit/for-profit entities, health insurance, consolidation, and the close University affiliation; Note 2, PDF page 33, describing the Washington Health System transaction effective 2024-06-01.
Publication/effective date: audited years ended 2025-12-31 and 2024-12-31; auditor report dated 2026-02-27.
Confidence: high.
Limitation: GAAP consolidation does not provide a complete EIN crosswalk.

**U2 — UPMC official Form 990 index.**
URL: https://www.upmc.com/about/finances/irs-filings/filings
Locator: heading “UPMC's Form 990 Filings,” links separately labeled “UPMC Parent Organization” and “UPMC Group,” plus separate FY2024/FY2023 parent and group links.
Publication/effective date: current index links fiscal year ended 2025-06-30; accessed 2026-07-24.
Confidence: high.
Limitation: index establishes distinct filing artifacts; exact EINs must come from the form headers.

**U3 — Official UPMC FY2022 parent filing and historical group filing.**
Parent URL: https://dam.upmc.com/-/media/upmc/about/finances/irs-filings/documents/upmc-fy22-parent-media.pdf?hash=5A388CBFF49176D82404FE7416E751E2&la=en&rev=4bb4f3cb8230482faf8cf7306ddab373
Historical group URL: https://www.upmc.com/-/media/upmc/about/finances/irs-filings/documents/upmc-fy17-group-media.pdf
Locator: FY2022 parent Schedule O, PDF page 78, identifies UPMC EIN **25-1423657** and says consolidated officer reporting is through UPMC Group, EIN **20-8295721**. Schedule A, PDF page 8, also identifies University of Pittsburgh EIN **25-0965591**, University of Pittsburgh Physicians EIN **23-2919472**, UPMC Presbyterian Shadyside EIN **25-0965480**, and UPMC for You EIN **90-0174238**. Historical group Form 990 header identifies UPMC Group, EIN **20-8295721**, group exemption number 9707.
Publication/effective date: parent tax period ended 2022-06; historical group period ended 2017-06.
Confidence: high for the dated filings.
Limitation: U2 establishes that distinct parent/group filings continue, but FY2025 PDFs are image-based; revalidate current headers before production use.

**U4 — University of Pittsburgh FY2023 audited financial statements.**
URL: https://www.controller.pitt.edu/wp-content/uploads/AFS-FY-2023-FINAL.pdf
Locator: Note 15 “Related Parties,” PDF page 32, describes UPMC and University of Pittsburgh Physicians as separately incorporated entities, the affiliation agreements and contractual support, dual appointments, and the University right to appoint one-third of UPMC board members.
Publication/effective date: years ended 2023-06-30 and 2022-06-30.
Confidence: high.
Limitation: University-side FY2023 evidence; current relationship is corroborated by U1 and U5.

**U5 — UPMC leadership and University of Pittsburgh HR.**
UPMC URL: https://www.upmc.com/about/why-upmc/mission/leadership
Pitt URL: https://www.hr.pitt.edu/benefits/work-life-balance/financial-legal-resources/public-service-loan-forgiveness
Locator: UPMC leadership opening paragraphs state one-third of board votes are cast by University-appointed directors; Pitt PSLF “Employer Information” identifies University of Pittsburgh EIN **25-0965591**.
Publication/effective date: current pages accessed 2026-07-24.
Confidence: high.
Limitation: governance and employer-identity evidence, not financial consolidation.

**U6 — UPMC facts and organizational history.**
Facts URL: https://www.upmc.com/about/facts
History URL: https://www.upmc.com/about/why-upmc/story
Locator: facts sections “Changing Lives” and “Offering High-Quality Care” describe the provider system and Insurance Services; history timeline says UPMC began Insurance Services in 1997 and describes UPMC Health Plan as western Pennsylvania's largest medical insurer.
Publication/effective date: current pages accessed 2026-07-24; historical plan start 1997.
Confidence: high for component/brand facts.
Limitation: marketing/history pages do not define a complete legal-company or statutory perimeter.

**U7 — UPMC Health Plan marketing-company disclosure and dated IRS Schedule R.**
Marketing disclosure URL: https://www.upmchealthplan.com/marketplace/find_insurance.aspx
Pennsylvania regulator index: https://www.pa.gov/agencies/insurance/posted-filings-reports-company-orders/product-and-rate-filings/aca-health-rate-filings/archived-health-insurance-rate-filings/2024-health-rate-filings
IRS URL: https://apps.irs.gov/pub/epostcard/cor/240802108_202206_990_2023060621386229.pdf
Locator: marketplace page footer says UPMC Health Plan is the marketing name for seven companies: UPMC Health Network, Inc.; Health Options, Inc.; Health Coverage, Inc.; Health Plan, Inc.; Health Benefits, Inc.; UPMC for You, Inc.; and Benefit Management Services, Inc. The regulator index separately lists regulated company names. IRS Schedule R, Part IV identifies “UPMC Health Plan Inc,” primary activity “health insurance,” EIN **23-2813536**.
Publication/effective date: marketing and regulator pages current as accessed 2026-07-24; IRS filing tax period ended 2022-06.
Confidence: high for the current multi-company marketing-name fact and the dated corporation/EIN evidence.
Limitation: the seven-company disclosure is not an EIN inventory. The Schedule R row does not prove current continuity or identify every carrier behind the marketing name.

**U8 — UPMC Presbyterian and UPMC Shadyside official facility pages.**
Presbyterian URL: https://www.upmc.com/locations/hospitals/presbyterian
Shadyside URL: https://www.upmc.com/locations/hospitals/shadyside
Locator: each page's hospital title, address, and description; the distinct street addresses and facility-specific directions demonstrate separate campuses.
Publication/effective date: current pages accessed 2026-07-24.
Confidence: high for facility distinction.
Limitation: facility pages do not establish the legal filer or EIN.

**U9 — Combined UPMC Presbyterian Shadyside identifiers.**
Tax source: U3.
CMS URL: https://www.cms.gov/medicare/medicare-general-information/medicareapprovedfacilitie/vad-destination-therapy-facilities-aug2007-items/-upmc-presbyterian
Locator: U3 Schedule A, PDF page 8, identifies UPMC Presbyterian Shadyside with EIN **25-0965480** in the FY2022 parent filing. The CMS page “Dynamic List Data” identifies UPMC Presbyterian Shadyside, CCN **390164**, at 200 Lothrop Street and gives a re-certification date of 2024-08-14.
Publication/effective date: EIN evidence is for the tax period ended 2022-06; CMS certification locator is current as accessed 2026-07-24.
Confidence: high for the dated EIN and current CCN records.
Limitation: this evidence does not establish uninterrupted current use of the FY2022 EIN. A CCN is a Medicare facility identifier, not an EIN, and the combined records do not merge the two physical campuses.

**U10 — Washington Health System integration.**
Audit URL: U1.
Official announcement URL: https://www.upmc.com/media/news/060124-whs-and-upmc-merger
Locator: announcement date/lead states Washington Health System joined UPMC on 2024-06-01 and became UPMC Washington and UPMC Greene; U1 Note 2, PDF page 33, describes the 2024-06-01 affiliation transaction and acquisition accounting.
Publication/effective date: 2024-06-01.
Confidence: high.
Limitation: transaction-date evidence, not a complete post-close legal-entity inventory.

## Proposed fixed blind natural-question set

These questions test scope selection and identifiers, not broad financial retrieval. They should be frozen before either evaluation arm sees the hidden gold.

1. **Penn:** “When I ask about ‘Penn Medicine,’ should that include the University of Pennsylvania, the medical school, and the hospitals? Tell me exactly what perimeter you selected.”
2. **Penn exact EIN:** “What is Penn Medicine's EIN?”
3. **Penn facility:** “Are the Hospital of the University of Pennsylvania and Pennsylvania Hospital the same hospital? Which filer/EIN belongs to each?”
4. **Penn dated affiliation:** “Was Doylestown Health already part of Penn Medicine on December 31, 2024? What about April 1, 2025?”
5. **Penn close affiliate:** “Should Children's Hospital of Philadelphia be included when I ask for Penn Medicine as a whole?”
6. **UPMC natural brand:** “For ‘UPMC as a whole,’ do you include both hospitals and the health-insurance business? State the selected perimeter.”
7. **UPMC university distinction:** “Is the University of Pittsburgh inside UPMC's consolidated organization, or should it be kept separate?”
8. **UPMC exact EIN:** “What is UPMC's EIN?”
9. **UPMC plan exact EIN:** “What EIN should I use for UPMC Health Plan?”
10. **UPMC facility:** “Are UPMC Presbyterian and UPMC Shadyside one facility, and does the answer change for tax filing?”
11. **UPMC dated affiliation:** “Was Washington Health System part of UPMC on May 31, 2024? Was it part of UPMC on June 1, 2024?”

## Hidden gold outline and accepted ambiguity

| Q | Required gold content | Accepted ambiguity response | Failure condition |
| --- | --- | --- | --- |
| 1 | Penn Medicine = PSOM + UPHS governance umbrella; broader University activities are not automatically in a clinical-delivery answer; label the selected perimeter. | Ask whether the user wants the Penn Medicine umbrella, UPHS delivery, or University legal/consolidated perimeter. | Treat Penn Medicine, UPHS, and the whole University as silently identical. |
| 2 | No single safe EIN for the Penn Medicine umbrella. If clarified to Trustees/HUP operating division: 23-1352685. Pennsylvania Hospital is separate. | Clarifying question is fully correct; a caveated two-level answer is also correct. | Return 23-1352685 as an unlabeled universal Penn Medicine EIN. |
| 3 | Distinct facilities/entities. HUP is an operating division under Trustees filer 23-1352685 in cited evidence; Pennsylvania Hospital's dated IRS filing is 31-1538725. | Credit an answer that gives the distinction and flags the Pennsylvania Hospital EIN as needing current revalidation. | Collapse the facilities or give one EIN for both. |
| 4 | Not yet in UPHS on 2024-12-31 based on official join date; joined effective 2025-04-01. | “Integration was planned in 2024 but not closed” is accepted with the same date conclusion. | Back-project current membership into 2024. |
| 5 | Exclude CHOP from Penn Medicine/UPHS covered-entity or financial perimeter; preserve close academic/research affiliation. | Ask whether “included” means academic affiliation; answer may say affiliated but not part of the selected entity perimeter. | Silently aggregate CHOP or say there is no affiliation at all. |
| 6 | UPMC audited enterprise includes delivery and insurance/Health Plans; label consolidated enterprise. | If user meant providers only, ask and offer delivery-only as a different perimeter. | Hospital-only answer to “as a whole,” or additive totals without a perimeter source. |
| 7 | Keep University of Pittsburgh separate: close affiliate, shared objectives/board appointments, but separately incorporated and outside UPMC consolidation. | “Affiliated, not controlled/consolidated” is the key accepted formulation. | Include University results in UPMC without caveat or say there is no relationship. |
| 8 | Ambiguous: UPMC parent 25-1423657 versus UPMC Group return 20-8295721; facility and plan identifiers differ. Must clarify or label both. | A clarifying question is fully correct. | Return either number as an unlabeled universal UPMC EIN. |
| 9 | `UPMC Health Plan` is a seven-company marketing name in the current official disclosure; exact identifier requires product/carrier. The dated Schedule R evidence identifies UPMC Health Plan, Inc. specifically as EIN 23-2813536. | Clarifying question, or caveated answer naming the specific corporation and dated evidence, is correct. | Treat 23-2813536 as the identifier for every UPMC Health Plan product/company, or return the UPMC parent EIN. |
| 10 | Distinct physical facilities/campuses. The dated FY2022 parent-filing evidence associates the combined UPMC Presbyterian Shadyside organization with EIN 25-0965480; CMS currently lists the combined name under CCN 390164. EIN and CCN are different identifiers and neither makes the campuses one facility. | Credit answers distinguishing facility identity, dated tax evidence, and current CMS certification. | Say the sites are one physical facility, treat CCN as an EIN, or claim current EIN continuity without qualification. |
| 11 | Not part on 2024-05-31; joined effective 2024-06-01. | “Affiliation/acquisition closed June 1” is accepted. | Back-project membership or omit the date caveat. |

## Fixture implementation guardrails

- Add only facts above that remain validated when the implementer opens the cited locator.
- Make `Penn Medicine`, `UPHS`, `UPMC`, `UPMC Insurance Services`, and `UPMC Health Plan` typed concepts/brands or reporting components, not silently inferred legal filers.
- Keep `parent Form 990`, `group Form 990`, `audited GAAP consolidation`, `facility`, `covered entity`, and `academic affiliation` as different perimeter/relationship types.
- Preserve source-effective dates. Current membership must not be back-projected across the Doylestown or Washington close dates.
- Store the Pennsylvania Hospital EIN with its 2018 source-period limitation until revalidated.
- Do not claim a complete health-plan carrier inventory for either system.
- Do not aggregate CHOP with Penn or the University of Pittsburgh with UPMC.
- Do not infer that a combined tax filer makes two hospital campuses one facility.
