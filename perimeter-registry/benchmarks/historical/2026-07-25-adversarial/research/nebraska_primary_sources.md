# Nebraska Medicine / UNMC perimeter evidence dossier

**Research access timestamp (UTC):** 2026-07-25T20:10:13Z
**Evidence policy:** Primary, first-party official sources only (Nebraska Medicine, University of Nebraska Medical Center, University of Nebraska Board of Regents/System, Nebraska DHHS, CMS/NPPES and direct IRS-hosted filings). No IRS Form 990 Evidence Service was queried or modified.
**Question:** What is the leanest defensible enterprise perimeter for Nebraska Medicine, and which similarly named or closely affiliated entities and identifiers must remain distinct?

## Executive determination

The defensible core is not a single-name/single-identifier organization.

1. **Nebraska Medicine** is both a patient-facing brand and, since 2016, the legal Nebraska public-benefit/nonprofit parent corporation for the integrated clinical enterprise. The current interim articles say Nebraska Medicine is the sole member of The Nebraska Medical Center and UNMC Physicians and coordinates and controls them. Official 2015 materials described the then-contemplated structure as a holding company for those two legacy organizations plus Bellevue Medical Center; the current articles and direct IRS filings supply the later legal/tax chain.
2. **Nebraska Medical Center** and **Bellevue Medical Center** are the two branded hospitals, but they are distinct Medicare-certified facilities and distinct NPI organizations. CMS currently records CCN 280013 for THE NEBRASKA MEDICAL CENTER and CCN 280132 for BELLEVUE MEDICAL CENTER. NPPES records Bellevue's legal business name as BELLEVUE MEDICAL CENTER LLC and its DBA as BELLEVUE MEDICAL CENTER - NEBRASKA MEDICINE.
3. **UNMC is not the same legal entity as Nebraska Medicine.** It is a campus of the University of Nebraska, Nebraska's public university system, and Nebraska Medicine's primary clinical/research/education partner. Shared logo, mission, campus, faculty/physician dual employment, HIPAA arrangements and University governance rights are strong affiliations, not a basis to collapse UNMC into a Nebraska Medicine legal-entity perimeter.
4. The **University of Nebraska Board of Regents**, not “UNMC” as a free-standing hospital operator, exercises the University member/governance relationship with Nebraska Medicine. The July 1–September 30, 2026 interim articles name the Regents the **sole legal member**. During the same period University and Omaha Community Foundation representatives have equal voting representation on Nebraska Medicine's board. OCF board appointment rights must not be mislabeled as completed corporate membership.
5. Direct IRS-hosted returns provide entity-specific EINs: **Nebraska Medicine 81-3158267**, **The Nebraska Medical Center 91-1858433**, **UNMC Physicians 47-0785575**, and **Bellevue Medical Center 20-4305186**. The Bellevue filing classifies it as a disregarded entity directly controlled by TNMC. A separate UNMC fundraiser form represents **47-0049123** as the tax ID of its named University payee. These numbers must remain attached only to the entities and source periods that state them.

### Registry recommendation

Use separate nodes and typed edges:

| Node | Lean disposition | Safe relationship label |
|---|---|---|
| Nebraska Medicine (Nebraska public benefit/nonprofit corporation / umbrella) | Include as enterprise anchor | EIN 81-3158267; sole member of TNMC and UNMC Physicians under current interim articles |
| The/Nebraska Medical Center | Include as hospital/operator node | EIN 91-1858433; Nebraska Medicine hospital; CCN 280013; sole member is Nebraska Medicine |
| Bellevue Medical Center LLC | Include as separate hospital/operator node beneath TNMC | EIN 20-4305186 for Bellevue Medical Center in TNMC's filing-period Schedule R; Nebraska Medicine hospital/DBA; CCN 280132; NPI 1609007525 |
| UNMC Physicians | Include only when physician-group scope is requested, and always as a distinct node | EIN 47-0785575; faculty medical group; Nebraska Medicine is sole member; do not inherit UNMC or hospital identifiers |
| University of Nebraska Medical Center (UNMC) | Exclude from the owned clinical-operator core; retain as an external affiliated/governance-related node | Public academic partner; campus of University of Nebraska; separate from Nebraska Medicine |
| Board of Regents of the University of Nebraska | Retain as member/governance node | Sole legal member of Nebraska Medicine during July 1–Sept. 30, 2026 interim articles; not a hospital CCN/EIN roll-up target |
| Omaha Community Foundation | Retain as interim governance-representative node as of access date | Equal board representation July 1–Sept. 30, 2026; prospective member status not yet proved complete |
| Clarkson Regional Health Services | Historical member; outside current core after June 30, 2026 | Former governing member; retain dated relationship and transaction history only |

## Findings and adversarial boundaries

### A. Nebraska Medicine: brand, predecessor and corporate layers

**Supported chronology (high confidence):**

- 1997: Clarkson Hospital and University Hospital merged to create Nebraska Health System.
- 2003: Nebraska Health System changed its name to The Nebraska Medical Center.
- 2014: The Nebraska Medical Center, Bellevue Medical Center and UNMC Physicians were clinically integrated under the Nebraska Medicine brand.
- 2016: Nebraska Medicine was officially established as a nonprofit corporation.

The official history therefore distinguishes at least four concepts that a registry must not flatten: the 1997 predecessor system, the hospital called The Nebraska Medical Center, the 2014 Nebraska Medicine brand/clinical integration, and the 2016 Nebraska Medicine nonprofit corporation.

**Historical structure evidence (medium-high confidence, dated limitation):** An October 28, 2015 UNMC governance update said a September 28 term sheet defined Nebraska Medicine as the holding company for The Nebraska Medical Center, Bellevue Medical Center and UNMC Physicians. The same article said definitive governance documents were still expected in 2016. This is strong primary evidence of intended structure, but not a substitute for current articles, bylaws or a current subsidiary schedule.

**Current corporate/governance evidence (high confidence):** The Board of Regents' January 22, 2026 statement calls Nebraska Medicine a 501(c)(3) not-for-profit created in 2016 by the University and Clarkson. The Board's June 29 interim articles, effective July 1–September 30, 2026, give the exact legal name “Nebraska Medicine,” classify it as a Nebraska public benefit corporation, and say it is a 501(c)(3) supporting organization formed to integrate The Nebraska Medical Center and UNMC Physicians. The articles say Nebraska Medicine is the sole member of and coordinates/controls both. Direct IRS-hosted returns corroborate the parent/controlled-organization relationship for the latest filing period located.

**Naming trap:** Nebraska Medicine's current media guidance says the proper patient-care hospital names are “Nebraska Medical Center” and “Bellevue Medical Center”; “Nebraska Medicine - Bellevue,” “UNMC Physicians,” “NMC” and “UNMC” are incorrect substitutes for Nebraska Medicine in patient-care reporting. This is brand-use guidance, not evidence that the legal entities or federal identifiers have merged.

### B. UNMC: close affiliation, separate public-university entity

**Public-university status (high confidence):** The University of Nebraska says it is Nebraska's only public university system and is made up of four campuses, including UNMC. UNMC says it became part of the University of Nebraska in 1902. UNMC is therefore a University campus/public academic health science center, not another name for a Nebraska Medicine hospital.

**Separation evidence (high confidence):** UNMC's June 11, 2015 official article states directly that UNMC and Nebraska Medicine are “distinctly separate organizations,” says Nebraska Medicine is UNMC's primary clinical teaching partner, and says UNMC remained a separate entity even though the two shared an emblem. The current Nebraska Medicine “About Us” and partner pages likewise call UNMC its research/education or primary clinical partner.

**Governance does not erase separation (high confidence):** The University's governance FAQ says Nebraska Medicine will remain a separate not-for-profit corporate entity with its own employees, benefits, policies, board, articles, bylaws and budget; Nebraska Medicine employees do not become state employees. It also acknowledges physician dual employment. Thus University member rights and operational/academic alignment are not a legal-entity merger.

**Perimeter answer:** For a Nebraska Medicine **owned/operated clinical enterprise** perimeter, exclude UNMC from the core and link it as a public academic partner plus a governance-related affiliate through the University/Board of Regents. For a broader **common-control/governance ecosystem** view, include the University Board of Regents and UNMC as separate nodes, never as aliases, subsidiaries, hospitals or shared-ID records.

### C. The two hospitals are not one provider record

**Hospital presentation (high confidence):** Nebraska Medicine's March 2026 fast facts identifies two hospitals and 809 licensed beds: Nebraska Medical Center (718) and Bellevue Medical Center (91). Its history page separately describes each hospital.

**CMS certification facts (high confidence; dataset modified 2026-04-28):**

| Facility record | Exact CMS facility ID / CCN | CMS ownership field | Address in CMS result | Do not infer |
|---|---:|---|---|---|
| THE NEBRASKA MEDICAL CENTER | 280013 | Voluntary non-profit - Private | 988102 Nebrasks Medical Center, Omaha, NE 68198 (CMS source contains “Nebrasks”) | Do not assign this CCN to Bellevue; do not silently “correct” the source address in an evidence fixture |
| BELLEVUE MEDICAL CENTER | 280132 | Proprietary | 2500 Bellevue Medical Center Dr, Bellevue, NE 68123 | “Proprietary” is CMS's facility classification, not proof that the Nebraska Medicine parent is for-profit |

**NPPES organization facts (high confidence for the records; currency limitation noted):**

- NPI **1609007525** is an active Type 2 organization with legal business name **BELLEVUE MEDICAL CENTER LLC**, DBA **BELLEVUE MEDICAL CENTER - NEBRASKA MEDICINE**, at 2500 Bellevue Medical Center Dr. Its NPPES record was last updated 2018-05-01, so it supports the enumerated name/DBA and separate organization record, but not a claim about current equity percentages.
- NPI **1356307581** is an active Type 2 organization whose NPPES legal business name is **NEBRASKA MEDICAL CENTER** and whose DBA is **THE NEBRASKA MEDICAL CENTER - NEBRASKA MEDICINE**. It was last updated and certified 2025-05-06. NPPES also lists a Bellevue-campus secondary practice location; that does not convert Bellevue Medical Center's separate CCN 280132 into CCN 280013.

**Operator/control evidence and limitation:** A 2013 UNMC article said Bellevue Medical Center was then jointly owned by The Nebraska Medical Center, UNMC Physicians and private physicians. A direct IRS-hosted TNMC return for 2022-07-01 through 2023-06-30 instead lists Bellevue Medical Center, EIN 20-4305186, as a disregarded entity with TNMC as its direct controlling entity. The current July 15, 2026 Nebraska DHHS roster names Bellevue Medical Center LLC as licensee. Use the later operator/control evidence for the supported filing period, but do not invent equity percentages or claim that an IRS disregarded-entity classification makes the licensed LLC disappear.

### D. HIPAA, shared care and identifiers do not prove entity identity

UNMC's current joint Notice of Privacy Practices separately enumerates The Nebraska Medical Center, The Bellevue Medical Center, UNMC, UNMC Physicians, Nebraska Pediatric Practice, Inc. and University Dental Associates. It then says these organizations designated themselves an affiliated covered entity for HIPAA and that Nebraska Medicine participates in organized health care arrangements.

This is affirmative evidence of operational/data-sharing affiliation **and** of separately named participants. A shared HIPAA ACE/OHCA, common notice, electronic record, logo, mission, campus, medical staff or patient-facing brand must not be used as an alias/ownership rule.

### E. Dated governance facts and current-state caution

| Effective/event date | Officially supported fact | Confidence / caution |
|---|---|---|
| 1997 | University of Nebraska and Clarkson/CRHS began dual-member governance of the predecessor system | High; repeated in 2026 Board releases |
| 2016 | Nebraska Medicine nonprofit corporation established | High |
| July 2024 | CRHS communicated intent to withdraw its membership | High |
| Jan. 15, 2026 | Regents approved a proposed transaction intended to leave the University as sole member after CRHS withdrawal | High as approval/plan; not by itself closing proof |
| June 29, 2026 | Regents approved amended/restated bylaws and articles for a temporary governance structure | High |
| After June 30, 2026 | Official June 24 release says CRHS “will have withdrawn and will no longer serve” as governing member | High, corroborated by the interim articles naming only the Regents as member |
| July 1–Sept. 30, 2026 | Interim articles name the Regents sole legal member; bylaws give University and OCF appointees two voting directors each | High; member status and director appointment rights are distinct |
| As of access date | OCF membership remained prospective in the latest official releases and OCF was not named a member in the effective interim articles | High; do not call OCF a completed corporate member/owner |

The January UNMC governance FAQ described sole-University governance as a planned end state. The effective June 29 governing documents now supply the precise interim state: the Regents are sole legal member, OCF has appointment rights for half the voting directors, and OCF corporate membership remains future work.

## Exact EIN and CCN evidence

| Identifier | Entity exactly named by source | Source meaning and period | Confidence | Prohibited aggregation |
|---|---|---|---|---|
| EIN 81-3158267 | Nebraska Medicine | Direct IRS-hosted 2023-07-01 through 2024-06-30 return, filed 2025-05-13 | High for taxpayer-filed return and period | Do not assign to TNMC, Bellevue, UNMC Physicians or UNMC |
| EIN 91-1858433 | The Nebraska Medical Center | Same IRS-hosted Schedule R names TNMC as controlled by Nebraska Medicine; 2021 employer form independently states the EIN | High | Do not assign to Bellevue, UNMC Physicians or the parent |
| EIN 47-0785575 | UNMC Physicians | Same IRS-hosted Schedule R names UNMC Physicians as controlled by Nebraska Medicine | High for return and period | Do not confuse with UNMC public-campus tax ID representation |
| EIN 20-4305186 | Bellevue Medical Center | Direct IRS-hosted TNMC return for 2022-07-01 through 2023-06-30 lists it as a disregarded entity directly controlled by TNMC | High for return and period | Do not replace with TNMC EIN even though federally disregarded; preserve licensed LLC node |
| Tax ID 47-0049123 | “The University of Nebraska-Medical Center” (fundraiser payee wording) | 2025 SHARING the Green registration form | High for the named payee representation | Do not assign to Nebraska Medicine, either hospital or UNMC Physicians; campus-versus-system scope remains unproved |
| CCN 280013 | THE NEBRASKA MEDICAL CENTER | CMS Hospital General Information; dataset modified 2026-04-28; current Nebraska DHHS roster corroborates | High | Do not roll into Bellevue Medical Center |
| CCN 280132 | BELLEVUE MEDICAL CENTER | CMS Hospital General Information; dataset modified 2026-04-28; current Nebraska DHHS roster corroborates | High | Do not roll into Nebraska Medical Center |

The IRS XML is a direct official archive of taxpayer-filed returns, not the repository's IRS 990 Evidence Service. No service was queried or modified. Return statements are primary taxpayer representations, not an IRS certificate of every legal conclusion.

## Primary-source ledger

All URLs below were accessed at the dossier timestamp unless a machine endpoint states its own record date.

1. **Nebraska Medicine, “History of Nebraska Medicine.”**
   URL: https://www.nebraskamed.com/about-us/nebraska-medicine-history
   Locator: `Timeline of key events` → `Nebraska Medicine milestones`; `About Nebraska Medical Center`; `About Bellevue Medical Center`.
   Source period: live page, includes milestones through 2026.
   Supports: 1997/2003/2014/2016 chronology; two-hospital distinctions and beds.
   Confidence: High. Limitation: official narrative/brand history, not articles of incorporation or an equity schedule.

2. **Nebraska Medicine, “Fast Facts about Nebraska Medicine.”**
   URL: https://www.nebraskamed.com/about-us/fast-facts
   Locator: `Facts`; specifically “Primary clinical partner,” “Two hospitals,” and licensed-bed bullets.
   Source period: facts updated March 2026; page contains a separate awards update note.
   Supports: enterprise presentation as two hospitals; 718/91 bed split; partner wording.
   Confidence: High for first-party current presentation.

3. **Nebraska Medicine, “Media Relations.”**
   URL: https://www.nebraskamed.com/about-us/news
   Locator: `Name Usage` and `Proper references for Nebraska Medicine hospitals`.
   Source period: live page at access.
   Supports: proper hospital brand names; warnings against patient-care aliases.
   Confidence: High for brand usage only; no legal-ownership inference.

4. **UNMC, “Shared emblem underscores interdependence,” June 11, 2015.**
   URL: https://www.unmc.edu/newsroom/2015/06/11/shared-emblem-underscores-interdependence/
   Locator: opening paragraphs beginning “The physical proximity…” through “UNMC…remains a separate entity.”
   Source period: published 2015-06-11, describing the 2014 brand launch.
   Supports: shared logo; explicit separate-entity status; three integrated legacy entities.
   Confidence: High, but dated; separation is also corroborated by 2026 governance materials.

5. **UNMC, “Nebraska Medicine governance and leadership update,” Oct. 28, 2015.**
   URL: https://www.unmc.edu/newsroom/2015/10/28/nebraska-medicine-governance-and-leadership-update/
   Locator: paragraphs describing the Sept. 28 term sheet and “holding company for legacy organizations.”
   Source period: 2015 term-sheet stage, before definitive 2016 documents.
   Supports: intended umbrella/holding-company structure.
   Confidence: Medium-high; limitation is expressly stated pre-definitive status.

6. **University of Nebraska System, “About the University” statement in June 29 release.**
   URL: https://nebraska.edu/news-and-events/news/2026/06/regents-approve-interim-structure
   Locator: paragraphs under headline; `About the University of Nebraska`.
   Source period: published 2026-06-29; temporary structure effective 2026-07-01 through 2026-09-30.
   Supports: amended articles/bylaws; equal University/OCF board representation; OCF membership still prospective; UNMC as one of four public-system campuses.
   Confidence: High.

7. **University of Nebraska System, “NU Regents to meet on Monday, June 29,” June 24, 2026.**
   URL: https://nebraska.edu/news-and-events/news/2026/06/nu-regents-to-meet-on-monday-june-29
   Locator: paragraphs beginning “The proposed actions…” and “As of June 30…”.
   Source period: published 2026-06-24, describing transition effective July 1.
   Supports: CRHS withdrawal date and interim OCF-representative period.
   Confidence: High for the University's official transition representation; closing instruments not independently examined.

8. **University of Nebraska System, Board statement, Jan. 22, 2026.**
   URL: https://nebraska.edu/news-and-events/news/2026/01/statement-by-the-university-of-nebraska-board-of-regents
   Locator: first four paragraphs; especially 2016 creation, 501(c)(3), articles' support/benefit purpose, and member capacities.
   Source period: 2026-01-22, during governance dispute/transition.
   Supports: nonprofit corporate form and member-purpose facts.
   Confidence: High for Board's first-party corporate statement; potentially advocacy-context source, so paired with formal Board materials.

9. **University of Nebraska Board of Regents, Jan. 2026 agenda with materials.**
   URL: https://nebraska.edu/_files/docs/regents/agendas-and-minutes/2026/january-9-board-meeting/agenda-with-materials-1-9-26.pdf
   Locator: PDF page 8 of 28, `TERM SHEET – SUMMARY OF THE PROPOSED TRANSACTIONS`, item 1 `Proposed Transaction`.
   Source period: term sheet dated 2026-01-02; non-binding until definitive agreements, as the document states.
   Supports: University and CRHS as corporate members; Nebraska nonprofit-corporation label; proposed sole-member transition.
   Confidence: High for proposal terms, not proof of closing.

10. **UNMC, “Nebraska Medicine Governance” FAQ.**
    URL: https://www.unmc.edu/news/nm-governance/index.html
    Locator: `Key Points You Should Know`; Q&A “Will Nebraska Medicine employees become University employees?”, “Will Nebraska Medicine become a state entity?”, and “Is Nebraska Medicine currently an independent health care system?”.
    Source period: January 2026 proposal context; superseded on interim board composition by June 2026 releases.
    Supports: separate corporate entity, staff/budget/board, dual employment and non-state-employee status.
    Confidence: High for separation; limitation: stale for current governance composition.

11. **UNMC, joint Notice of Privacy Practices.**
    URL: https://www.unmc.edu/patientcare/hipaa/notice-privacy-practices.html
    Locator: opening section `This Notice applies to the following organizations and clinics`, through ACE/OHCA paragraphs.
    Source period: current live notice at access.
    Supports: separately listed organizations; HIPAA ACE/OHCA affiliation.
    Confidence: High; limitation: HIPAA arrangement is not an ownership determination.

12. **CMS Provider Data Catalog, “Hospital General Information.”**
    Dataset page: https://data.cms.gov/provider-data/dataset/xubh-q36u
    Metadata endpoint: https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xubh-q36u
    Bellevue exact query: https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?conditions%5B0%5D%5Bproperty%5D=facility_name&conditions%5B0%5D%5Bvalue%5D=BELLEVUE%20MEDICAL%20CENTER&conditions%5B0%5D%5Boperator%5D=%3D
    Nebraska exact query: https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?conditions%5B0%5D%5Bproperty%5D=facility_name&conditions%5B0%5D%5Bvalue%5D=THE%20NEBRASKA%20MEDICAL%20CENTER&conditions%5B0%5D%5Boperator%5D=%3D
    Locator: JSON `results[0].facility_id`, `facility_name`, `address`, `hospital_type`, `hospital_ownership`; metadata `modified` and `issued`.
    Source period: dataset issued 2025-01-08, modified 2026-04-28 (Provider Data page says release 2026-05-13).
    Supports: distinct CCNs 280132 and 280013 and current facility fields.
    Confidence: High. Limitation: CMS facility ownership classification is not a complete corporate family tree.

13. **CMS NPPES Read API, Bellevue Medical Center.**
    URL: https://npiregistry.cms.hhs.gov/api/?version=2.1&number=1609007525&pretty=on
    Locator: `results[0].basic.organization_name`, `other_names`, `number`, `addresses`, `last_updated`.
    Source period: enumerated 2009-07-28; last updated 2018-05-01; active at access.
    Supports: Bellevue Medical Center LLC legal business name, Nebraska Medicine DBA and separate NPI.
    Confidence: High for NPPES record; limited currency for ownership.

14. **CMS NPPES Read API, Nebraska Medical Center.**
    URL: https://npiregistry.cms.hhs.gov/api/?version=2.1&number=1356307581&pretty=on
    Locator: `results[0].basic`, `other_names`, `number`, `practiceLocations`.
    Source period: last updated/certified 2025-05-06; active at access.
    Supports: separate NPI record, name/DBA, Bellevue secondary location caution.
    Confidence: High for NPPES record.

15. **Nebraska Medicine, New Colleague Orientation booklet.**
    URL: https://welcome.nebraskamed.com/wp-content/uploads/2021/07/NCO-Booklet_Final_063021.pdf
    Locator: PDF page 13, Marketplace employer coverage form, fields 3–5.
    Source period: file dated/finalized 2021-06-30 and posted July 2021.
    Supports: employer “The Nebraska Medical Center dba Nebraska Medicine,” EIN 91-1858433.
    Confidence: High for the stated employer and period; not current parent/Bellevue EIN proof.

16. **UNMC, 2025 SHARING the Green registration form.**
    URL: https://www.unmc.edu/newsroom/wp-content/uploads/2025/09/2025-STG-Registration-Form.pdf
    Locator: PDF page 1, payment instructions and bottom line `Tax ID # 47-0049123`.
    Source period: 2025 event form, posted September 2025.
    Supports: tax ID represented for named UNMC fundraiser payee.
    Confidence: High within that transaction context; not proof the number is exclusively scoped to the UNMC campus.

17. **UNMC, “New leadership structure announced for OneTeam,” Nov. 13, 2013.**
    URL: https://www.unmc.edu/newsroom/2013/11/13/new-leadership-structure-announced-for-oneteam/
    Locator: opening paragraphs describing separate administrations, UNMC Physicians and Bellevue ownership.
    Source period: published 2013-11-13, before 2014 brand and 2016 corporation.
    Supports: historical equity/administrative distinction only.
    Confidence: High historically; do not project its ownership statement into 2026.

18. **University of Nebraska Board of Regents, June 29, 2026 agenda with current interim governing documents.**
    URL: https://nebraska.edu/_files/docs/regents/agendas-and-minutes/2026/june-29-board-meeting/agenda-6-29-26-with-materials.pdf
    Locator: PDF p. 6 §3.02 (interim directors); pp. 21–23, Articles I, II and IV; p. 23 §5.1; p. 25 §5.4.
    Source period: articles/bylaws effective 2026-07-01 through 2026-09-30.
    Supports: exact legal name; public-benefit/501(c)(3) supporting-corporation form; Nebraska Medicine sole membership/control of TNMC and UNMC Physicians; Regents sole legal member; OCF director appointment rights but not corporate membership.
    Confidence: Very high; approved governing documents. Limitation: expressly interim and due to expire/supersede after September 30.

19. **IRS direct electronic Form 990 archive, Nebraska Medicine return.**
    Index URL: https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv
    Archive URL: https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_05A.zip
    Locator: internal file `202511339349307596_public.xml`; `ReturnHeader/Filer`; `IRS990ScheduleR/IdRelatedTaxExemptOrgGrp`.
    Source period: tax period 2023-07-01 through 2024-06-30; filed 2025-05-13.
    Supports: Nebraska Medicine EIN 81-3158267; TNMC EIN 91-1858433 and UNMC Physicians EIN 47-0785575 as controlled organizations with Nebraska Medicine as direct controlling entity; formation year 2016.
    Confidence: High for the taxpayer-filed return. Limitation: taxpayer representation for that period, not an IRS adjudication or a current W-9. Access was direct to IRS files, not through the repository's IRS 990 Evidence Service.

20. **IRS direct electronic Form 990 archive, The Nebraska Medical Center return.**
    Index URL: https://apps.irs.gov/pub/epostcard/990/xml/2024/index_2024.csv
    Archive URL: https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_05A.zip
    Locator: internal file `2024_TEOS_XML_05A/202441349349307864_public.xml`; `ReturnHeader/Filer`; `IRS990ScheduleR/IdDisregardedEntitiesGrp`.
    Source period: tax period 2022-07-01 through 2023-06-30; filed 2024-05-13.
    Supports: TNMC EIN 91-1858433; Bellevue Medical Center EIN 20-4305186, primary activity `ACUTE CARE`, as a disregarded entity directly controlled by TNMC.
    Confidence: High for the taxpayer-filed return. Limitation: tax classification/control for the stated period does not eliminate the separately licensed LLC or prove current equity percentages.

21. **Nebraska DHHS, current Hospital Roster.**
    URL: https://dhhs.ne.gov/licensure/Documents/Hospital%20Roster.pdf
    Locator: PDF p. 3, Bellevue; PDF p. 20, Omaha/The Nebraska Medical Center.
    Source period: roster updated 2026-07-15.
    Supports: Bellevue Medical Center LLC as licensee, state license H000115, CCN 280132 and 91 licensed beds; The Nebraska Medical Center as licensee, state license 260011, CCN 280013 and 690 licensed beds.
    Confidence: Very high for current state licensure. Limitation: Nebraska Medicine's consumer page says 718 acute-care beds for Nebraska Medical Center; use the DHHS figure specifically when the field means current regulatory licensed beds.

## Unresolved ambiguity and mandatory non-aggregation rules

1. **Entity EINs are resolved only for stated return periods:** Preserve Nebraska Medicine 81-3158267, TNMC 91-1858433, UNMC Physicians 47-0785575 and Bellevue Medical Center 20-4305186 as distinct identifiers. Do not cross-populate them.
2. **Bellevue disregarded status is not entity erasure:** TNMC's filing reports Bellevue as a federally disregarded entity directly controlled by TNMC; the state roster still names Bellevue Medical Center LLC as the licensed hospital operator.
3. **UNMC tax ID scope unresolved:** The official form supports a named payee/tax-ID representation, not a conclusion about whether 47-0049123 is campus-only, system-wide or used by additional University units.
4. **OCF status time-boxed:** The Regents were sole legal member under the effective interim articles while OCF appointed half the voting directors. Board seat, appointment right, member and owner are not synonyms.
5. **Bellevue equity percentages unresolved:** Preserve Bellevue Medical Center LLC as a distinct operator record. Do not perpetuate 2013 ownership percentages or holders as current; the later tax-control chain supersedes that history for its filing period.
6. **UNMC Physicians remains separate from UNMC:** Current articles make Nebraska Medicine its sole member; do not collapse the faculty practice group into UNMC, either hospital or another EIN.
7. **HIPAA is not corporate consolidation:** ACE/OHCA/common notice, EHR and data sharing do not establish ownership or a single taxpayer/provider record.
8. **Shared brand is not identifier inheritance:** Nebraska Medicine, Nebraska Medical Center, Bellevue Medical Center, UNMC and UNMC Physicians are not interchangeable aliases for EIN/CCN/NPI purposes.
9. **A secondary practice address is not a hospital CCN:** Nebraska Medical Center NPI 1356307581 having a Bellevue secondary location does not erase Bellevue's distinct CCN 280132 and NPI 1609007525.
10. **Historical dates describe different layers:** 1997 predecessor formation, 2014 brand integration and 2016 nonprofit-corporation establishment are all valid and should not be forced into one “founded” field without a qualifier.

## Candidate blind-scope questions and gold answers

1. **Q:** Is UNMC an alias or owned hospital of Nebraska Medicine?
   **Gold:** No. UNMC is a separate public University of Nebraska campus and academic/research partner. Link it to Nebraska Medicine as partner/governance-related affiliate; do not merge legal identifiers.

2. **Q:** Does the University governance relationship make Nebraska Medicine a state entity or its employees state employees?
   **Gold:** No. Official governance materials say Nebraska Medicine remains a separate 501(c)(3) with its own board, budget, policies and employees; physician dual employment does not collapse the employers.

3. **Q:** What are the hospital CCNs?
   **Gold:** The Nebraska Medical Center is 280013; Bellevue Medical Center is 280132. They must remain separate facility/provider records.

4. **Q:** May CCN 280013 be assigned to all Nebraska Medicine hospitals because the brand presents one system?
   **Gold:** No. CMS maintains a distinct Bellevue facility record under 280132.

5. **Q:** What legal operator name is directly supported for Bellevue?
   **Gold:** Current Nebraska DHHS licensure and NPPES support BELLEVUE MEDICAL CENTER LLC; NPPES gives the DBA BELLEVUE MEDICAL CENTER - NEBRASKA MEDICINE and NPI 1609007525. TNMC's direct IRS-hosted return lists Bellevue as its disregarded, directly controlled acute-care entity for the filing period. Equity percentages remain unresolved.

6. **Q:** What EIN belongs to Bellevue Medical Center?
   **Gold:** 20-4305186, directly stated in TNMC's IRS-hosted Schedule R for 2022-07-01 through 2023-06-30. Preserve the Bellevue LLC/operator node and do not substitute TNMC's EIN.

7. **Q:** What does EIN 91-1858433 prove?
   **Gold:** It is The Nebraska Medical Center's EIN, supported by a direct IRS-hosted return and an official employee coverage form. It is not Nebraska Medicine parent EIN 81-3158267, Bellevue EIN 20-4305186 or UNMC Physicians EIN 47-0785575.

8. **Q:** What does tax ID 47-0049123 prove?
   **Gold:** A 2025 UNMC event form identifies it for the payee “The University of Nebraska-Medical Center.” It must not be assigned to Nebraska Medicine or its hospitals, and its campus-versus-system scope remains unproved.

9. **Q:** Are Nebraska Medicine, Nebraska Medical Center and UNMC the same name at different levels?
   **Gold:** No. Nebraska Medicine is the brand and 2016 parent corporation (EIN 81-3158267); The Nebraska Medical Center is a controlled hospital/operator (EIN 91-1858433, CCN 280013); UNMC is the separate public academic campus.

10. **Q:** Should UNMC Physicians be merged into UNMC because it is the faculty group?
    **Gold:** No. Treat it as a separately named faculty practice organization, EIN 47-0785575. Current articles make Nebraska Medicine its sole member. Include it when physician-group scope is requested and preserve its identifiers.

11. **Q:** Is Omaha Community Foundation a completed 50% owner/member as of July 25, 2026?
    **Gold:** No under the effective interim articles reviewed. The Regents are sole legal member; OCF appoints two of four voting directors for July 1–Sept. 30 while possible future OCF membership is negotiated.

12. **Q:** Is Clarkson Regional Health Services still a current governing member?
    **Gold:** No under the effective interim articles, which name only the Regents as member. Preserve Clarkson's 1997–June 2026 relationship historically.

13. **Q:** Does the joint HIPAA notice establish a single legal healthcare entity?
    **Gold:** No. The notice separately lists the participating organizations and identifies ACE/OHCA arrangements for permitted sharing and joint operations.

14. **Q:** What is the safest one-line perimeter conclusion?
    **Gold:** Regents → Nebraska Medicine parent → TNMC and UNMC Physicians, with Bellevue as TNMC's separately licensed/disregarded hospital operator; UNMC remains a separate public academic partner, and every EIN, CCN and NPI stays on its own node.

## Post-freeze source correction — 2026-07-25T21:16:00Z

The manual-style baseline located the official University of Nebraska 2026 Group
Dental Plan: https://nebraska.edu/_files/docs/faculty-staff/health-benefits/dental-insurance.pdf.
PDF pp. 36-37 name University of Nebraska as employer, identify the plan as University
of Nebraska (Board of Regents of the University of Nebraska), give employer EIN
47-0049123, and name the Board as plan sponsor. This resolves the dossier's earlier
campus-versus-system ambiguity: 47-0049123 belongs on a University/Board legal record,
not on the UNMC campus node. Frozen benchmark inputs and scores remain preserved; the
live fixture was corrected after the run.
