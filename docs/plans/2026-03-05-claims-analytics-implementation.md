# Claims & Service Line Analytics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build MCP server 11 (port 8012) providing 5 tools for claims-based hospital analytics using CMS Medicare Provider Utilization PUFs.

**Architecture:** Single `data_loaders.py` downloads 3 years of inpatient + outpatient PUFs as CSV, caches as Parquet, queries with DuckDB. Static bundled CSVs for DRG-to-service-line mapping and DRG weights. Follows workforce-analytics server patterns exactly.

**Tech Stack:** FastMCP, httpx, pandas, duckdb, pydantic, parquet (zstd)

---

### Task 1: Create directory structure and `__init__.py`

**Files:**
- Create: `servers/claims_analytics/__init__.py`
- Create: `servers/claims_analytics/data/` (directory)
- Create symlink: `servers/claims-analytics` → `servers/claims_analytics`

**Step 1: Create the package directory and init file**

```bash
mkdir -p servers/claims_analytics/data
touch servers/claims_analytics/__init__.py
```

**Step 2: Create the kebab-case symlink**

```bash
cd servers && ln -s claims_analytics claims-analytics && cd ..
```

**Step 3: Verify structure**

```bash
ls -la servers/claims-analytics/
ls servers/claims_analytics/
```

Expected: symlink pointing to `claims_analytics`, containing `__init__.py` and `data/`.

**Step 4: Commit**

```bash
git add servers/claims_analytics/__init__.py
git commit -m "chore: scaffold claims-analytics server directory"
```

---

### Task 2: Create static data files

**Files:**
- Create: `servers/claims_analytics/data/drg_service_line_map.csv`
- Create: `servers/claims_analytics/data/drg_weights_fy2024.csv`

**Step 1: Create DRG-to-service-line mapping CSV**

Create `servers/claims_analytics/data/drg_service_line_map.csv` with ~150 of the most common MS-DRGs mapped to service lines. The full file should have columns: `drg_code,service_line`

Service line categories:
- Cardiovascular
- Orthopedics
- Neurosciences
- Oncology
- General Surgery
- Pulmonary / Respiratory
- Gastroenterology
- Renal / Urology
- Women's Health / OB-GYN
- Neonatal
- Behavioral Health
- Trauma / Burns
- Transplant
- Rehabilitation
- Infectious Disease
- Endocrine / Metabolic
- ENT / Ophthalmology
- Other Medical
- Other Surgical

Key DRG ranges to map (based on MDC):
- MDC 01 (DRGs 020-103): Neurosciences
- MDC 04 (DRGs 163-208): Pulmonary / Respiratory
- MDC 05 (DRGs 215-316): Cardiovascular
- MDC 06 (DRGs 326-395): Gastroenterology
- MDC 07 (DRGs 405-446): Endocrine / Metabolic
- MDC 08 (DRGs 453-517): Orthopedics
- MDC 09 (DRGs 570-585): Dermatology → Other Medical
- MDC 10 (DRGs 592-607): Endocrine / Metabolic
- MDC 11 (DRGs 614-645): Renal / Urology
- MDC 12 (DRGs 652-675): Oncology (Male)
- MDC 13 (DRGs 707-730): Women's Health / OB-GYN
- MDC 14 (DRGs 734-761): Women's Health / OB-GYN (obstetric)
- MDC 15 (DRGs 765-776): Neonatal
- MDC 16 (DRGs 789-816): Oncology (Blood/Immunological)
- MDC 17 (DRGs 820-849): Oncology (Myeloproliferative)
- MDC 18 (DRGs 853-872): Infectious Disease
- MDC 19 (DRGs 876-887): Behavioral Health
- MDC 20 (DRGs 901-923): Behavioral Health (substance abuse)
- MDC 21 (DRGs 927-929): Trauma / Burns
- MDC 22 (DRGs 933-935): Trauma / Burns (extensive)
- MDC 23 (DRGs 939-941): Other Medical
- MDC 24 (DRGs 945-946): Other Surgical (HIV)
- MDC 25 (DRGs 949-950): Other Medical (polytrauma)
- Pre-MDC (DRGs 001-017): Transplant / ECMO / Trach

Build a comprehensive map covering the full DRG range. Include all DRGs from 001-999 by assigning based on MDC classification. Use the DRG-to-MDC mapping from CMS MS-DRG v41. Format:

```csv
drg_code,service_line
001,Transplant
002,Transplant
003,Transplant
004,Transplant
005,Transplant
006,Transplant
007,Transplant
008,Transplant
010,Transplant
011,Transplant
012,Transplant
013,Transplant
014,Transplant
016,Transplant
017,Transplant
020,Neurosciences
021,Neurosciences
022,Neurosciences
023,Neurosciences
024,Neurosciences
025,Neurosciences
026,Neurosciences
027,Neurosciences
028,Neurosciences
029,Neurosciences
030,Neurosciences
031,Neurosciences
032,Neurosciences
033,Neurosciences
034,Neurosciences
035,Neurosciences
036,Neurosciences
037,Neurosciences
038,Neurosciences
039,Neurosciences
040,Neurosciences
041,Neurosciences
042,Neurosciences
052,ENT / Ophthalmology
053,ENT / Ophthalmology
054,ENT / Ophthalmology
055,ENT / Ophthalmology
056,ENT / Ophthalmology
057,ENT / Ophthalmology
058,ENT / Ophthalmology
059,ENT / Ophthalmology
060,ENT / Ophthalmology
061,ENT / Ophthalmology
062,ENT / Ophthalmology
063,ENT / Ophthalmology
064,ENT / Ophthalmology
065,ENT / Ophthalmology
066,ENT / Ophthalmology
067,ENT / Ophthalmology
068,ENT / Ophthalmology
069,ENT / Ophthalmology
070,ENT / Ophthalmology
071,ENT / Ophthalmology
072,ENT / Ophthalmology
073,ENT / Ophthalmology
074,ENT / Ophthalmology
163,Pulmonary / Respiratory
164,Pulmonary / Respiratory
165,Pulmonary / Respiratory
166,Pulmonary / Respiratory
167,Pulmonary / Respiratory
168,Pulmonary / Respiratory
175,Pulmonary / Respiratory
176,Pulmonary / Respiratory
177,Pulmonary / Respiratory
178,Pulmonary / Respiratory
179,Pulmonary / Respiratory
180,Pulmonary / Respiratory
181,Pulmonary / Respiratory
182,Pulmonary / Respiratory
183,Pulmonary / Respiratory
184,Pulmonary / Respiratory
185,Pulmonary / Respiratory
186,Pulmonary / Respiratory
187,Pulmonary / Respiratory
188,Pulmonary / Respiratory
189,Pulmonary / Respiratory
190,Pulmonary / Respiratory
191,Pulmonary / Respiratory
192,Pulmonary / Respiratory
193,Pulmonary / Respiratory
194,Pulmonary / Respiratory
195,Pulmonary / Respiratory
196,Pulmonary / Respiratory
197,Pulmonary / Respiratory
198,Pulmonary / Respiratory
199,Pulmonary / Respiratory
200,Pulmonary / Respiratory
201,Pulmonary / Respiratory
202,Pulmonary / Respiratory
203,Pulmonary / Respiratory
204,Pulmonary / Respiratory
205,Pulmonary / Respiratory
206,Pulmonary / Respiratory
207,Pulmonary / Respiratory
208,Pulmonary / Respiratory
215,Cardiovascular
216,Cardiovascular
217,Cardiovascular
218,Cardiovascular
219,Cardiovascular
220,Cardiovascular
221,Cardiovascular
222,Cardiovascular
223,Cardiovascular
224,Cardiovascular
225,Cardiovascular
226,Cardiovascular
227,Cardiovascular
228,Cardiovascular
229,Cardiovascular
230,Cardiovascular
231,Cardiovascular
232,Cardiovascular
233,Cardiovascular
234,Cardiovascular
235,Cardiovascular
236,Cardiovascular
237,Cardiovascular
238,Cardiovascular
239,Cardiovascular
240,Cardiovascular
241,Cardiovascular
242,Cardiovascular
243,Cardiovascular
244,Cardiovascular
245,Cardiovascular
246,Cardiovascular
247,Cardiovascular
248,Cardiovascular
249,Cardiovascular
250,Cardiovascular
251,Cardiovascular
252,Cardiovascular
253,Cardiovascular
254,Cardiovascular
255,Cardiovascular
256,Cardiovascular
257,Cardiovascular
258,Cardiovascular
259,Cardiovascular
260,Cardiovascular
261,Cardiovascular
262,Cardiovascular
263,Cardiovascular
264,Cardiovascular
265,Cardiovascular
266,Cardiovascular
267,Cardiovascular
268,Cardiovascular
269,Cardiovascular
270,Cardiovascular
271,Cardiovascular
272,Cardiovascular
280,Cardiovascular
281,Cardiovascular
282,Cardiovascular
283,Cardiovascular
284,Cardiovascular
285,Cardiovascular
286,Cardiovascular
287,Cardiovascular
288,Cardiovascular
289,Cardiovascular
290,Cardiovascular
291,Cardiovascular
292,Cardiovascular
293,Cardiovascular
294,Cardiovascular
295,Cardiovascular
296,Cardiovascular
297,Cardiovascular
298,Cardiovascular
299,Cardiovascular
300,Cardiovascular
301,Cardiovascular
302,Cardiovascular
303,Cardiovascular
304,Cardiovascular
305,Cardiovascular
306,Cardiovascular
307,Cardiovascular
308,Cardiovascular
309,Cardiovascular
310,Cardiovascular
311,Cardiovascular
312,Cardiovascular
313,Cardiovascular
314,Cardiovascular
315,Cardiovascular
316,Cardiovascular
326,Gastroenterology
327,Gastroenterology
328,Gastroenterology
329,Gastroenterology
330,Gastroenterology
331,Gastroenterology
332,Gastroenterology
333,Gastroenterology
334,Gastroenterology
335,Gastroenterology
336,Gastroenterology
337,Gastroenterology
338,Gastroenterology
339,Gastroenterology
340,Gastroenterology
341,Gastroenterology
342,Gastroenterology
343,Gastroenterology
344,Gastroenterology
345,Gastroenterology
346,Gastroenterology
347,Gastroenterology
348,Gastroenterology
349,Gastroenterology
350,Gastroenterology
351,Gastroenterology
352,Gastroenterology
353,Gastroenterology
354,Gastroenterology
355,Gastroenterology
356,Gastroenterology
357,Gastroenterology
368,Gastroenterology
369,Gastroenterology
370,Gastroenterology
371,Gastroenterology
372,Gastroenterology
373,Gastroenterology
374,Gastroenterology
375,Gastroenterology
376,Gastroenterology
377,Gastroenterology
378,Gastroenterology
379,Gastroenterology
380,Gastroenterology
381,Gastroenterology
382,Gastroenterology
383,Gastroenterology
384,Gastroenterology
385,Gastroenterology
386,Gastroenterology
387,Gastroenterology
388,Gastroenterology
389,Gastroenterology
390,Gastroenterology
391,Gastroenterology
392,Gastroenterology
393,Gastroenterology
394,Gastroenterology
395,Gastroenterology
405,Endocrine / Metabolic
406,Endocrine / Metabolic
407,Endocrine / Metabolic
408,Endocrine / Metabolic
409,Endocrine / Metabolic
410,Endocrine / Metabolic
411,Endocrine / Metabolic
412,Endocrine / Metabolic
413,Endocrine / Metabolic
414,Endocrine / Metabolic
415,Endocrine / Metabolic
416,Endocrine / Metabolic
417,Endocrine / Metabolic
418,Endocrine / Metabolic
419,Endocrine / Metabolic
420,Endocrine / Metabolic
421,Endocrine / Metabolic
422,Endocrine / Metabolic
423,Endocrine / Metabolic
424,Endocrine / Metabolic
425,Endocrine / Metabolic
432,Endocrine / Metabolic
433,Endocrine / Metabolic
434,Endocrine / Metabolic
435,Endocrine / Metabolic
436,Endocrine / Metabolic
437,Endocrine / Metabolic
438,Endocrine / Metabolic
439,Endocrine / Metabolic
440,Endocrine / Metabolic
441,Endocrine / Metabolic
442,Endocrine / Metabolic
443,Endocrine / Metabolic
444,Endocrine / Metabolic
445,Endocrine / Metabolic
446,Endocrine / Metabolic
453,Orthopedics
454,Orthopedics
455,Orthopedics
456,Orthopedics
457,Orthopedics
458,Orthopedics
459,Orthopedics
460,Orthopedics
461,Orthopedics
462,Orthopedics
463,Orthopedics
464,Orthopedics
465,Orthopedics
466,Orthopedics
467,Orthopedics
468,Orthopedics
469,Orthopedics
470,Orthopedics
471,Orthopedics
472,Orthopedics
473,Orthopedics
474,Orthopedics
475,Orthopedics
476,Orthopedics
477,Orthopedics
478,Orthopedics
479,Orthopedics
480,Orthopedics
481,Orthopedics
482,Orthopedics
483,Orthopedics
484,Orthopedics
485,Orthopedics
486,Orthopedics
487,Orthopedics
488,Orthopedics
489,Orthopedics
490,Orthopedics
491,Orthopedics
492,Orthopedics
493,Orthopedics
494,Orthopedics
495,Orthopedics
496,Orthopedics
497,Orthopedics
498,Orthopedics
499,Orthopedics
500,Orthopedics
501,Orthopedics
502,Orthopedics
503,Orthopedics
504,Orthopedics
505,Orthopedics
506,Orthopedics
507,Orthopedics
508,Orthopedics
509,Orthopedics
510,Orthopedics
511,Orthopedics
512,Orthopedics
513,Orthopedics
514,Orthopedics
515,Orthopedics
516,Orthopedics
517,Orthopedics
533,General Surgery
534,General Surgery
535,General Surgery
536,General Surgery
537,General Surgery
538,General Surgery
539,General Surgery
540,General Surgery
541,General Surgery
542,General Surgery
543,General Surgery
544,General Surgery
545,General Surgery
546,General Surgery
547,General Surgery
548,General Surgery
549,General Surgery
550,General Surgery
551,General Surgery
552,General Surgery
553,General Surgery
554,General Surgery
555,General Surgery
556,General Surgery
557,General Surgery
558,General Surgery
559,General Surgery
560,General Surgery
561,General Surgery
562,General Surgery
563,General Surgery
564,General Surgery
565,General Surgery
570,Other Medical
571,Other Medical
572,Other Medical
573,Other Medical
574,Other Medical
575,Other Medical
576,Other Medical
577,Other Medical
578,Other Medical
579,Other Medical
580,Other Medical
581,Other Medical
582,Other Medical
583,Other Medical
592,Endocrine / Metabolic
593,Endocrine / Metabolic
594,Endocrine / Metabolic
595,Endocrine / Metabolic
596,Endocrine / Metabolic
597,Endocrine / Metabolic
598,Endocrine / Metabolic
599,Endocrine / Metabolic
600,Endocrine / Metabolic
601,Endocrine / Metabolic
602,Endocrine / Metabolic
603,Endocrine / Metabolic
604,Endocrine / Metabolic
605,Endocrine / Metabolic
606,Endocrine / Metabolic
607,Endocrine / Metabolic
614,Renal / Urology
615,Renal / Urology
616,Renal / Urology
617,Renal / Urology
618,Renal / Urology
619,Renal / Urology
620,Renal / Urology
621,Renal / Urology
622,Renal / Urology
623,Renal / Urology
624,Renal / Urology
625,Renal / Urology
626,Renal / Urology
627,Renal / Urology
628,Renal / Urology
629,Renal / Urology
630,Renal / Urology
637,Renal / Urology
638,Renal / Urology
639,Renal / Urology
640,Renal / Urology
641,Renal / Urology
642,Renal / Urology
643,Renal / Urology
644,Renal / Urology
645,Renal / Urology
652,Oncology
653,Oncology
654,Oncology
655,Oncology
656,Oncology
657,Oncology
658,Oncology
659,Oncology
660,Oncology
661,Oncology
662,Oncology
663,Oncology
664,Oncology
665,Oncology
666,Oncology
667,Oncology
668,Oncology
669,Oncology
670,Oncology
671,Oncology
672,Oncology
673,Oncology
674,Oncology
675,Oncology
707,Women's Health / OB-GYN
708,Women's Health / OB-GYN
709,Women's Health / OB-GYN
710,Women's Health / OB-GYN
711,Women's Health / OB-GYN
712,Women's Health / OB-GYN
713,Women's Health / OB-GYN
714,Women's Health / OB-GYN
715,Women's Health / OB-GYN
716,Women's Health / OB-GYN
717,Women's Health / OB-GYN
718,Women's Health / OB-GYN
722,Women's Health / OB-GYN
723,Women's Health / OB-GYN
724,Women's Health / OB-GYN
725,Women's Health / OB-GYN
726,Women's Health / OB-GYN
727,Women's Health / OB-GYN
728,Women's Health / OB-GYN
729,Women's Health / OB-GYN
730,Women's Health / OB-GYN
734,Women's Health / OB-GYN
735,Women's Health / OB-GYN
736,Women's Health / OB-GYN
737,Women's Health / OB-GYN
738,Women's Health / OB-GYN
739,Women's Health / OB-GYN
740,Women's Health / OB-GYN
741,Women's Health / OB-GYN
742,Women's Health / OB-GYN
743,Women's Health / OB-GYN
744,Women's Health / OB-GYN
745,Women's Health / OB-GYN
746,Women's Health / OB-GYN
747,Women's Health / OB-GYN
748,Women's Health / OB-GYN
749,Women's Health / OB-GYN
750,Women's Health / OB-GYN
754,Women's Health / OB-GYN
755,Women's Health / OB-GYN
756,Women's Health / OB-GYN
757,Women's Health / OB-GYN
758,Women's Health / OB-GYN
759,Women's Health / OB-GYN
760,Women's Health / OB-GYN
761,Women's Health / OB-GYN
765,Neonatal
766,Neonatal
767,Neonatal
768,Neonatal
769,Neonatal
770,Neonatal
774,Neonatal
775,Neonatal
776,Neonatal
789,Oncology
790,Oncology
791,Oncology
792,Oncology
793,Oncology
794,Oncology
795,Oncology
796,Oncology
797,Oncology
798,Oncology
799,Oncology
800,Oncology
801,Oncology
802,Oncology
803,Oncology
804,Oncology
808,Oncology
809,Oncology
810,Oncology
811,Oncology
812,Oncology
813,Oncology
814,Oncology
815,Oncology
816,Oncology
820,Oncology
821,Oncology
822,Oncology
823,Oncology
824,Oncology
825,Oncology
826,Oncology
827,Oncology
828,Oncology
829,Oncology
830,Oncology
834,Oncology
835,Oncology
836,Oncology
837,Oncology
838,Oncology
839,Oncology
840,Oncology
841,Oncology
842,Oncology
843,Oncology
844,Oncology
845,Oncology
846,Oncology
847,Oncology
848,Oncology
849,Oncology
853,Infectious Disease
854,Infectious Disease
855,Infectious Disease
856,Infectious Disease
857,Infectious Disease
858,Infectious Disease
862,Infectious Disease
863,Infectious Disease
864,Infectious Disease
865,Infectious Disease
866,Infectious Disease
867,Infectious Disease
868,Infectious Disease
869,Infectious Disease
870,Infectious Disease
871,Infectious Disease
872,Infectious Disease
876,Behavioral Health
877,Behavioral Health
878,Behavioral Health
879,Behavioral Health
880,Behavioral Health
881,Behavioral Health
882,Behavioral Health
883,Behavioral Health
884,Behavioral Health
885,Behavioral Health
886,Behavioral Health
887,Behavioral Health
894,Behavioral Health
895,Behavioral Health
896,Behavioral Health
897,Behavioral Health
901,Behavioral Health
902,Behavioral Health
903,Behavioral Health
904,Behavioral Health
905,Behavioral Health
906,Behavioral Health
907,Behavioral Health
908,Behavioral Health
909,Behavioral Health
910,Behavioral Health
911,Behavioral Health
912,Behavioral Health
913,Behavioral Health
914,Behavioral Health
915,Behavioral Health
916,Behavioral Health
917,Behavioral Health
918,Behavioral Health
919,Behavioral Health
920,Behavioral Health
921,Behavioral Health
922,Behavioral Health
923,Behavioral Health
927,Trauma / Burns
928,Trauma / Burns
929,Trauma / Burns
933,Trauma / Burns
934,Trauma / Burns
935,Trauma / Burns
939,Other Medical
940,Other Medical
941,Other Medical
945,Infectious Disease
946,Infectious Disease
949,Other Medical
950,Other Medical
955,Other Medical
956,Other Medical
957,Other Medical
958,Other Medical
959,Other Medical
963,Other Medical
964,Other Medical
965,Other Medical
969,Other Medical
970,Other Medical
974,Other Medical
975,Other Medical
976,Other Medical
977,Other Medical
981,Other Medical
982,Other Medical
983,Other Medical
984,Other Medical
985,Other Medical
986,Other Medical
987,Other Medical
988,Other Medical
989,Other Medical
998,Other Medical
999,Other Medical
```

Note: The implementing agent should generate the complete CSV covering all valid MS-DRGs (v41). The above is the skeleton; fill in any gaps within each MDC range. If a DRG code doesn't exist, skip it — the Inpatient PUF only contains DRG codes that had discharges.

**Step 2: Create DRG weights CSV**

Create `servers/claims_analytics/data/drg_weights_fy2024.csv` with columns: `drg_code,weight`

Source the weights from the CMS IPPS FY2024 Final Rule Table 5. The implementing agent should:
1. Download Table 5 from `https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/acute-inpatient-files-download/files-fy-2024-final-rule-correction-notice`
2. Extract `drg_code` and `weight` columns
3. Save as CSV

If the download is unavailable, create a representative subset with commonly used DRGs and weights. The key DRGs and approximate weights:

```csv
drg_code,weight
001,25.4988
002,25.4988
003,17.8851
004,12.0720
005,12.0720
006,7.3483
007,5.3750
008,7.5118
010,4.3893
011,4.3893
012,2.9651
013,2.9651
014,10.8811
016,5.3395
017,5.3395
020,4.7613
021,3.2175
...
470,1.7394
...
```

The implementing agent should build the complete file by scraping or bundling all ~800 DRGs with weights.

**Step 3: Commit**

```bash
git add servers/claims_analytics/data/
git commit -m "feat(claims-analytics): add static DRG mapping and weight data files"
```

---

### Task 3: Create Pydantic response models

**Files:**
- Create: `servers/claims_analytics/models.py`

**Step 1: Write models.py**

```python
"""Pydantic models for claims & service line analytics server."""

from pydantic import BaseModel, Field


# --- Tool 1: get_inpatient_volumes ---

class DRGDetail(BaseModel):
    """Detail for a single DRG at a provider."""

    drg_code: str = ""
    drg_description: str = ""
    service_line: str = ""
    discharges: int = 0
    avg_charges: float = 0.0
    avg_total_payment: float = 0.0
    avg_medicare_payment: float = 0.0


class ServiceLineSummary(BaseModel):
    """Aggregated summary for one service line."""

    service_line: str = ""
    discharges: int = 0
    pct_of_total: float = 0.0
    avg_charges: float = 0.0
    avg_medicare_payment: float = 0.0


class InpatientVolumesResponse(BaseModel):
    """Response from get_inpatient_volumes."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    year: str = ""
    total_discharges: int = 0
    total_drgs: int = 0
    service_line_summary: list[ServiceLineSummary] = Field(default_factory=list)
    drg_details: list[DRGDetail] = Field(default_factory=list)


# --- Tool 2: get_outpatient_volumes ---

class APCDetail(BaseModel):
    """Detail for a single APC at a provider."""

    apc_code: str = ""
    apc_description: str = ""
    services: int = 0
    avg_charges: float = 0.0
    avg_total_payment: float = 0.0
    avg_medicare_payment: float = 0.0


class OutpatientVolumesResponse(BaseModel):
    """Response from get_outpatient_volumes."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    year: str = ""
    total_services: int = 0
    total_apcs: int = 0
    apc_details: list[APCDetail] = Field(default_factory=list)


# --- Tool 3: trend_service_lines ---

class ServiceLineTrend(BaseModel):
    """Multi-year trend for one inpatient service line."""

    service_line: str = ""
    volumes_by_year: dict[str, int] = Field(default_factory=dict)
    yoy_change_pct: dict[str, float] = Field(default_factory=dict)
    cagr_pct: float = 0.0


class OutpatientTrend(BaseModel):
    """Multi-year trend for one outpatient APC."""

    apc_code: str = ""
    apc_description: str = ""
    volumes_by_year: dict[str, int] = Field(default_factory=dict)
    yoy_change_pct: dict[str, float] = Field(default_factory=dict)
    cagr_pct: float = 0.0


class ServiceLineTrendResponse(BaseModel):
    """Response from trend_service_lines."""

    ccn: str = ""
    provider_name: str = ""
    years: list[str] = Field(default_factory=list)
    inpatient_trends: list[ServiceLineTrend] = Field(default_factory=list)
    outpatient_trends: list[OutpatientTrend] | None = None


# --- Tool 4: compute_case_mix ---

class ServiceLineAcuity(BaseModel):
    """Acuity metrics for one service line."""

    service_line: str = ""
    discharges: int = 0
    avg_drg_weight: float = 0.0
    pct_of_total_weight: float = 0.0


class DRGWeightContribution(BaseModel):
    """A DRG's contribution to total case mix weight."""

    drg_code: str = ""
    drg_description: str = ""
    service_line: str = ""
    discharges: int = 0
    drg_weight: float = 0.0
    total_weight_contribution: float = 0.0
    pct_of_total_weight: float = 0.0


class CaseMixResponse(BaseModel):
    """Response from compute_case_mix."""

    ccn: str = ""
    provider_name: str = ""
    year: str = ""
    case_mix_index: float = 0.0
    total_discharges: int = 0
    service_line_acuity: list[ServiceLineAcuity] = Field(default_factory=list)
    top_drgs_by_weight: list[DRGWeightContribution] = Field(default_factory=list)


# --- Tool 5: analyze_market_volumes ---

class ServiceLineShare(BaseModel):
    """Service line breakdown for a provider in market context."""

    service_line: str = ""
    discharges: int = 0
    market_share_pct: float = 0.0


class ProviderMarketShare(BaseModel):
    """One provider's market share within the defined geography."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    total_discharges: int = 0
    market_share_pct: float = 0.0
    service_line_breakdown: list[ServiceLineShare] = Field(default_factory=list)


class ServiceLineMarketTotal(BaseModel):
    """Total market volume for one service line."""

    service_line: str = ""
    total_discharges: int = 0
    pct_of_market: float = 0.0
    top_provider_ccn: str = ""
    top_provider_name: str = ""


class MarketVolumesResponse(BaseModel):
    """Response from analyze_market_volumes."""

    year: str = ""
    total_market_discharges: int = 0
    total_providers: int = 0
    provider_shares: list[ProviderMarketShare] = Field(default_factory=list)
    service_line_totals: list[ServiceLineMarketTotal] = Field(default_factory=list)
```

**Step 2: Verify models import cleanly**

```bash
cd /mnt/d/Coding\ Projects/healthcare-data-mcp
python -c "from servers.claims_analytics.models import InpatientVolumesResponse, CaseMixResponse; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add servers/claims_analytics/models.py
git commit -m "feat(claims-analytics): add Pydantic response models for 5 tools"
```

---

### Task 4: Create service_lines.py — DRG mapping and case mix logic

**Files:**
- Create: `servers/claims_analytics/service_lines.py`

**Step 1: Write service_lines.py**

```python
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
    """Load DRG→service-line mapping from bundled CSV."""
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
        logger.info("Loaded %d DRG→service-line mappings", len(_sl_map))
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
```

**Step 2: Verify it loads**

```bash
python -c "from servers.claims_analytics.service_lines import map_drg_to_service_line, compute_cmi; print(map_drg_to_service_line('470')); print(compute_cmi([('470', 100)]))"
```

Expected: `Orthopedics` and a float (or `Other Medical` / `1.0` if data files aren't populated yet).

**Step 3: Commit**

```bash
git add servers/claims_analytics/service_lines.py
git commit -m "feat(claims-analytics): add DRG mapping and case mix computation"
```

---

### Task 5: Create data_loaders.py — bulk download and Parquet caching

**Files:**
- Create: `servers/claims_analytics/data_loaders.py`

**Step 1: Write data_loaders.py**

```python
"""Bulk data loaders for CMS Medicare Provider Utilization PUFs.

Downloads inpatient and outpatient PUF CSV files from data.cms.gov,
converts to Parquet with zstd compression, and queries with DuckDB.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "claims-analytics"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_CACHE_TTL_DAYS = 90

# CMS data.cms.gov download URLs for Inpatient PUF (by Provider and Service)
# Pattern: MUP_INP_RY{release_year}_P03_V10_DY{discharge_year}_PrvSvc.CSV
INPATIENT_URLS: dict[str, str] = {
    "2023": "https://data.cms.gov/sites/default/files/2025-05/ca1c9013-8c7c-4560-a4a1-28cf7e43ccc8/MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV",
    "2022": "https://data.cms.gov/sites/default/files/2024-05/0b6cc46f-0264-4a07-b2c4-be3a34fd0498/MUP_INP_RY24_P03_V10_DY22_PrvSvc.CSV",
    "2021": "https://data.cms.gov/sites/default/files/2023-05/3e0e2616-8ff3-4d64-b10b-07273e90d8ff/MUP_INP_RY23_P03_V10_DY21_PrvSvc.CSV",
}

# CMS data.cms.gov download URLs for Outpatient PUF (by Provider and Service)
OUTPATIENT_URLS: dict[str, str] = {
    "2023": "https://data.cms.gov/sites/default/files/2025-08/bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv",
    "2022": "https://data.cms.gov/sites/default/files/2024-08/8b607225-c733-4f89-9490-9a84a5687604/MUP_OUT_RY24_P04_V10_DY22_Prov_Svc.csv",
    "2021": "https://data.cms.gov/sites/default/files/2023-08/d42dfca1-844a-4tried-a7e0-bcdafc7a0727/MUP_OUT_RY23_P04_V10_DY21_Prov_Svc.csv",
}

# Available years (most recent first)
AVAILABLE_YEARS = ["2023", "2022", "2021"]
LATEST_YEAR = "2023"


def _cache_path(dataset: str, year: str) -> Path:
    """Get Parquet cache path for a dataset and year."""
    return _CACHE_DIR / f"{dataset}_dy{year[-2:]}.parquet"


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    """Check if a cached file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


async def _download_and_cache_csv(url: str, cache_path: Path, dataset_name: str) -> bool:
    """Download CSV from CMS and cache as Parquet."""
    logger.info("Downloading %s from %s ...", dataset_name, url[:80])
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        csv_path = _CACHE_DIR / f"{cache_path.stem}_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(cache_path, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("%s cached: %d records -> %s", dataset_name, len(df), cache_path.name)
        return True

    except Exception as e:
        logger.warning("Failed to download %s: %s", dataset_name, e)
        return False


async def ensure_inpatient_cached(year: str = LATEST_YEAR) -> bool:
    """Ensure inpatient PUF for a given year is downloaded and cached."""
    path = _cache_path("inpatient", year)
    if _is_cache_valid(path):
        return True

    url = INPATIENT_URLS.get(year)
    if not url:
        logger.warning("No inpatient PUF URL for year %s", year)
        return False

    return await _download_and_cache_csv(url, path, f"Inpatient PUF DY{year}")


async def ensure_outpatient_cached(year: str = LATEST_YEAR) -> bool:
    """Ensure outpatient PUF for a given year is downloaded and cached."""
    path = _cache_path("outpatient", year)
    if _is_cache_valid(path):
        return True

    url = OUTPATIENT_URLS.get(year)
    if not url:
        logger.warning("No outpatient PUF URL for year %s", year)
        return False

    return await _download_and_cache_csv(url, path, f"Outpatient PUF DY{year}")


async def ensure_all_years_cached(include_outpatient: bool = True) -> list[str]:
    """Cache all available years. Returns list of years successfully cached."""
    cached_years = []
    for year in AVAILABLE_YEARS:
        inp_ok = await ensure_inpatient_cached(year)
        if include_outpatient:
            out_ok = await ensure_outpatient_cached(year)
            if inp_ok and out_ok:
                cached_years.append(year)
        elif inp_ok:
            cached_years.append(year)
    return cached_years


def _get_con_with_view(dataset: str, year: str) -> duckdb.DuckDBPyConnection | None:
    """Create DuckDB connection with a view for the cached Parquet file."""
    path = _cache_path(dataset, year)
    if not path.exists():
        return None
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{path}')")
    return con


def _detect_columns(con: duckdb.DuckDBPyConnection) -> dict[str, str | None]:
    """Detect column names dynamically (CMS data has inconsistent naming)."""
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='data'"
    ).fetchall()]

    return {
        "ccn": next((c for c in cols if c in (
            "rndrng_prvdr_ccn", "prvdr_ccn", "provider_ccn", "ccn"
        )), None),
        "provider_name": next((c for c in cols if c in (
            "rndrng_prvdr_org_name", "prvdr_org_name", "provider_name", "hospital_name"
        )), None),
        "state": next((c for c in cols if c in (
            "rndrng_prvdr_state_abrvtn", "prvdr_state_abrvtn", "state"
        )), None),
        "drg_code": next((c for c in cols if c in (
            "drg_cd", "drg_code", "ms_drg_cd"
        )), None),
        "drg_desc": next((c for c in cols if c in (
            "drg_desc", "drg_description", "ms_drg_desc"
        )), None),
        "discharges": next((c for c in cols if c in (
            "tot_dschrgs", "total_discharges", "discharges"
        )), None),
        "avg_charges": next((c for c in cols if c in (
            "avg_submtd_chrgs", "avg_submitted_charges", "avg_charges"
        )), None),
        "avg_total_payment": next((c for c in cols if c in (
            "avg_tot_pymt_amt", "avg_total_payment", "avg_tot_payment"
        )), None),
        "avg_medicare_payment": next((c for c in cols if c in (
            "avg_mdcr_pymt_amt", "avg_medicare_payment", "avg_mdcr_payment"
        )), None),
        # Outpatient-specific
        "apc_code": next((c for c in cols if c in (
            "apc_cd", "apc_code", "apc"
        )), None),
        "apc_desc": next((c for c in cols if c in (
            "apc_desc", "apc_description"
        )), None),
        "services": next((c for c in cols if c in (
            "outptnt_srvcs", "outpatient_services", "services", "capc_srvcs"
        )), None),
    }


def query_inpatient(
    year: str = LATEST_YEAR,
    ccn: str = "",
    ccns: list[str] | None = None,
    drg_code: str = "",
) -> list[dict]:
    """Query cached inpatient PUF data.

    Args:
        year: Discharge year.
        ccn: Single CCN to filter.
        ccns: List of CCNs (for market analysis).
        drg_code: Filter to specific DRG.
    """
    con = _get_con_with_view("inpatient", year)
    if con is None:
        return []

    try:
        col_map = _detect_columns(con)
        ccn_col = col_map["ccn"]
        if not ccn_col:
            con.close()
            return []

        where_parts: list[str] = []
        params: list = []

        if ccn:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        elif ccns:
            placeholders = ", ".join(["?"] * len(ccns))
            where_parts.append(f"TRIM({ccn_col}) IN ({placeholders})")
            params.extend([c.strip() for c in ccns])

        drg_col = col_map["drg_code"]
        if drg_code and drg_col:
            where_parts.append(f"TRIM({drg_col}) = ?")
            params.append(drg_code.strip())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        rows = con.execute(f"SELECT * FROM data WHERE {where}", params).fetchdf()
        con.close()

        results = []
        for _, row in rows.iterrows():
            def val(key: str) -> str:
                col = col_map.get(key)
                return str(row.get(col, "")).strip() if col and col in row.index else ""

            def fval(key: str) -> float:
                v = val(key)
                try:
                    return float(v.replace(",", "")) if v else 0.0
                except ValueError:
                    return 0.0

            def ival(key: str) -> int:
                return int(fval(key))

            results.append({
                "ccn": val("ccn"),
                "provider_name": val("provider_name"),
                "state": val("state"),
                "drg_code": val("drg_code"),
                "drg_desc": val("drg_desc"),
                "discharges": ival("discharges"),
                "avg_charges": fval("avg_charges"),
                "avg_total_payment": fval("avg_total_payment"),
                "avg_medicare_payment": fval("avg_medicare_payment"),
            })

        return results

    except Exception as e:
        logger.warning("Inpatient query failed: %s", e)
        con.close()
        return []


def query_outpatient(
    year: str = LATEST_YEAR,
    ccn: str = "",
    ccns: list[str] | None = None,
    apc_code: str = "",
) -> list[dict]:
    """Query cached outpatient PUF data."""
    con = _get_con_with_view("outpatient", year)
    if con is None:
        return []

    try:
        col_map = _detect_columns(con)
        ccn_col = col_map["ccn"]
        if not ccn_col:
            con.close()
            return []

        where_parts: list[str] = []
        params: list = []

        if ccn:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        elif ccns:
            placeholders = ", ".join(["?"] * len(ccns))
            where_parts.append(f"TRIM({ccn_col}) IN ({placeholders})")
            params.extend([c.strip() for c in ccns])

        apc_col = col_map["apc_code"]
        if apc_code and apc_col:
            where_parts.append(f"TRIM({apc_col}) = ?")
            params.append(apc_code.strip())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        rows = con.execute(f"SELECT * FROM data WHERE {where}", params).fetchdf()
        con.close()

        results = []
        for _, row in rows.iterrows():
            def val(key: str) -> str:
                col = col_map.get(key)
                return str(row.get(col, "")).strip() if col and col in row.index else ""

            def fval(key: str) -> float:
                v = val(key)
                try:
                    return float(v.replace(",", "")) if v else 0.0
                except ValueError:
                    return 0.0

            def ival(key: str) -> int:
                return int(fval(key))

            results.append({
                "ccn": val("ccn"),
                "provider_name": val("provider_name"),
                "state": val("state"),
                "apc_code": val("apc_code"),
                "apc_desc": val("apc_desc"),
                "services": ival("services"),
                "avg_charges": fval("avg_charges"),
                "avg_total_payment": fval("avg_total_payment"),
                "avg_medicare_payment": fval("avg_medicare_payment"),
            })

        return results

    except Exception as e:
        logger.warning("Outpatient query failed: %s", e)
        con.close()
        return []
```

**Step 2: Verify it imports cleanly**

```bash
python -c "from servers.claims_analytics import data_loaders; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add servers/claims_analytics/data_loaders.py
git commit -m "feat(claims-analytics): add bulk data loaders for inpatient/outpatient PUFs"
```

---

### Task 6: Create server.py — FastMCP with 5 tools

**Files:**
- Create: `servers/claims_analytics/server.py`

**Step 1: Write server.py**

```python
"""Claims & Service Line Analytics MCP Server.

Provides tools for inpatient discharge volumes, outpatient procedure volumes,
multi-year service line trends, case mix computation, and market volume analysis.
All data sourced from CMS Medicare Provider Utilization PUFs.
"""

import json
import logging
import os as _os
from mcp.server.fastmcp import FastMCP

from . import data_loaders, service_lines  # pyright: ignore[reportAttributeAccessIssue]
from .models import (
    APCDetail,
    CaseMixResponse,
    DRGDetail,
    DRGWeightContribution,
    InpatientVolumesResponse,
    MarketVolumesResponse,
    OutpatientTrend,
    OutpatientVolumesResponse,
    ProviderMarketShare,
    ServiceLineAcuity,
    ServiceLineMarketTotal,
    ServiceLineShare,
    ServiceLineSummary,
    ServiceLineTrend,
    ServiceLineTrendResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "claims-analytics"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8012"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Tool 1: get_inpatient_volumes
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_inpatient_volumes(
    ccn: str, drg_code: str = "", service_line: str = "", year: str = "",
) -> str:
    """Get inpatient discharge volumes by DRG and service line for a hospital.

    Uses CMS Medicare Inpatient Hospitals PUF (by Provider and Service).

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        drg_code: Filter to a specific MS-DRG code (e.g. "470").
        service_line: Filter to a service line (e.g. "Cardiovascular", "Orthopedics").
        year: Discharge year ("2021", "2022", "2023"). Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccn=ccn, drg_code=drg_code)
        if not rows:
            return json.dumps({"error": f"No inpatient data found for CCN: {ccn}"})

        # Map DRGs to service lines
        for r in rows:
            r["service_line"] = service_lines.map_drg_to_service_line(r["drg_code"])

        # Apply service line filter
        if service_line:
            rows = [r for r in rows if r["service_line"].lower() == service_line.lower()]
            if not rows:
                return json.dumps({"error": f"No data for service line '{service_line}' at CCN: {ccn}"})

        # Build DRG details
        drg_details = [
            DRGDetail(
                drg_code=r["drg_code"],
                drg_description=r["drg_desc"],
                service_line=r["service_line"],
                discharges=r["discharges"],
                avg_charges=r["avg_charges"],
                avg_total_payment=r["avg_total_payment"],
                avg_medicare_payment=r["avg_medicare_payment"],
            )
            for r in rows
        ]

        # Aggregate by service line
        sl_totals: dict[str, dict] = {}
        total_discharges = 0
        for r in rows:
            sl = r["service_line"]
            total_discharges += r["discharges"]
            if sl not in sl_totals:
                sl_totals[sl] = {"discharges": 0, "charge_sum": 0.0, "payment_sum": 0.0, "count": 0}
            sl_totals[sl]["discharges"] += r["discharges"]
            sl_totals[sl]["charge_sum"] += r["avg_charges"] * r["discharges"]
            sl_totals[sl]["payment_sum"] += r["avg_medicare_payment"] * r["discharges"]
            sl_totals[sl]["count"] += 1

        sl_summary = []
        for sl, t in sorted(sl_totals.items(), key=lambda x: x[1]["discharges"], reverse=True):
            sl_summary.append(ServiceLineSummary(
                service_line=sl,
                discharges=t["discharges"],
                pct_of_total=round(t["discharges"] / total_discharges * 100, 1) if total_discharges else 0,
                avg_charges=round(t["charge_sum"] / t["discharges"], 2) if t["discharges"] else 0,
                avg_medicare_payment=round(t["payment_sum"] / t["discharges"], 2) if t["discharges"] else 0,
            ))

        response = InpatientVolumesResponse(
            ccn=ccn,
            provider_name=rows[0]["provider_name"] if rows else "",
            state=rows[0]["state"] if rows else "",
            year=yr,
            total_discharges=total_discharges,
            total_drgs=len(drg_details),
            service_line_summary=sl_summary,
            drg_details=sorted(drg_details, key=lambda d: d.discharges, reverse=True),
        )
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("get_inpatient_volumes failed")
        return json.dumps({"error": f"get_inpatient_volumes failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_outpatient_volumes
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_outpatient_volumes(
    ccn: str, apc_code: str = "", year: str = "",
) -> str:
    """Get outpatient procedure volumes by APC for a hospital.

    Uses CMS Medicare Outpatient Hospitals PUF (by Provider and Service).

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        apc_code: Filter to a specific APC code.
        year: Discharge year ("2021", "2022", "2023"). Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_outpatient_cached(yr)

        rows = data_loaders.query_outpatient(year=yr, ccn=ccn, apc_code=apc_code)
        if not rows:
            return json.dumps({"error": f"No outpatient data found for CCN: {ccn}"})

        total_services = sum(r["services"] for r in rows)

        apc_details = [
            APCDetail(
                apc_code=r["apc_code"],
                apc_description=r["apc_desc"],
                services=r["services"],
                avg_charges=r["avg_charges"],
                avg_total_payment=r["avg_total_payment"],
                avg_medicare_payment=r["avg_medicare_payment"],
            )
            for r in rows
        ]

        response = OutpatientVolumesResponse(
            ccn=ccn,
            provider_name=rows[0]["provider_name"] if rows else "",
            state=rows[0]["state"] if rows else "",
            year=yr,
            total_services=total_services,
            total_apcs=len(apc_details),
            apc_details=sorted(apc_details, key=lambda a: a.services, reverse=True),
        )
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("get_outpatient_volumes failed")
        return json.dumps({"error": f"get_outpatient_volumes failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: trend_service_lines
# ---------------------------------------------------------------------------
@mcp.tool()
async def trend_service_lines(
    ccn: str, service_line: str = "", include_outpatient: bool = True,
) -> str:
    """Get multi-year volume trends by service line for a hospital (3-year).

    Shows year-over-year volume changes and compound annual growth rates.

    Args:
        ccn: CMS Certification Number (6-digit).
        service_line: Filter to one service line (e.g. "Cardiovascular").
        include_outpatient: Include outpatient APC trends (default True).
    """
    try:
        cached_years = await data_loaders.ensure_all_years_cached(include_outpatient)
        if not cached_years:
            return json.dumps({"error": "Failed to download PUF data for trend analysis"})

        years_sorted = sorted(cached_years)
        provider_name = ""

        # Inpatient trends by service line
        sl_by_year: dict[str, dict[str, int]] = {}
        for yr in years_sorted:
            rows = data_loaders.query_inpatient(year=yr, ccn=ccn)
            if rows and not provider_name:
                provider_name = rows[0]["provider_name"]
            for r in rows:
                sl = service_lines.map_drg_to_service_line(r["drg_code"])
                if service_line and sl.lower() != service_line.lower():
                    continue
                if sl not in sl_by_year:
                    sl_by_year[sl] = {}
                sl_by_year[sl][yr] = sl_by_year[sl].get(yr, 0) + r["discharges"]

        inpatient_trends = []
        for sl, volumes in sorted(sl_by_year.items()):
            yoy: dict[str, float] = {}
            sorted_yrs = sorted(volumes.keys())
            for i in range(1, len(sorted_yrs)):
                prev = volumes[sorted_yrs[i - 1]]
                curr = volumes[sorted_yrs[i]]
                if prev > 0:
                    yoy[sorted_yrs[i]] = round((curr - prev) / prev * 100, 1)

            # CAGR
            cagr = 0.0
            if len(sorted_yrs) >= 2:
                first_vol = volumes[sorted_yrs[0]]
                last_vol = volumes[sorted_yrs[-1]]
                n_years = int(sorted_yrs[-1]) - int(sorted_yrs[0])
                if first_vol > 0 and n_years > 0:
                    cagr = round(((last_vol / first_vol) ** (1 / n_years) - 1) * 100, 1)

            inpatient_trends.append(ServiceLineTrend(
                service_line=sl,
                volumes_by_year=volumes,
                yoy_change_pct=yoy,
                cagr_pct=cagr,
            ))

        # Outpatient trends by APC
        outpatient_trends = None
        if include_outpatient:
            apc_by_year: dict[str, dict] = {}
            for yr in years_sorted:
                rows = data_loaders.query_outpatient(year=yr, ccn=ccn)
                for r in rows:
                    apc = r["apc_code"]
                    if apc not in apc_by_year:
                        apc_by_year[apc] = {"desc": r["apc_desc"], "volumes": {}}
                    apc_by_year[apc]["volumes"][yr] = (
                        apc_by_year[apc]["volumes"].get(yr, 0) + r["services"]
                    )

            outpatient_trends = []
            for apc, data in sorted(apc_by_year.items()):
                volumes = data["volumes"]
                yoy: dict[str, float] = {}
                sorted_yrs = sorted(volumes.keys())
                for i in range(1, len(sorted_yrs)):
                    prev = volumes[sorted_yrs[i - 1]]
                    curr = volumes[sorted_yrs[i]]
                    if prev > 0:
                        yoy[sorted_yrs[i]] = round((curr - prev) / prev * 100, 1)

                cagr = 0.0
                if len(sorted_yrs) >= 2:
                    first_vol = volumes[sorted_yrs[0]]
                    last_vol = volumes[sorted_yrs[-1]]
                    n_years = int(sorted_yrs[-1]) - int(sorted_yrs[0])
                    if first_vol > 0 and n_years > 0:
                        cagr = round(((last_vol / first_vol) ** (1 / n_years) - 1) * 100, 1)

                outpatient_trends.append(OutpatientTrend(
                    apc_code=apc,
                    apc_description=data["desc"],
                    volumes_by_year=volumes,
                    yoy_change_pct=yoy,
                    cagr_pct=cagr,
                ))

        response = ServiceLineTrendResponse(
            ccn=ccn,
            provider_name=provider_name,
            years=years_sorted,
            inpatient_trends=sorted(inpatient_trends, key=lambda t: sum(t.volumes_by_year.values()), reverse=True),
            outpatient_trends=(
                sorted(outpatient_trends, key=lambda t: sum(t.volumes_by_year.values()), reverse=True)[:50]
                if outpatient_trends else None
            ),
        )
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("trend_service_lines failed")
        return json.dumps({"error": f"trend_service_lines failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: compute_case_mix
# ---------------------------------------------------------------------------
@mcp.tool()
async def compute_case_mix(ccn: str, year: str = "") -> str:
    """Compute case mix index and acuity analysis by service line for a hospital.

    Uses inpatient discharge data with CMS IPPS DRG relative weights.

    Args:
        ccn: CMS Certification Number (6-digit).
        year: Discharge year. Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccn=ccn)
        if not rows:
            return json.dumps({"error": f"No inpatient data found for CCN: {ccn}"})

        # Compute overall CMI
        drg_discharges = [(r["drg_code"], r["discharges"]) for r in rows]
        cmi = service_lines.compute_cmi(drg_discharges)

        total_discharges = sum(r["discharges"] for r in rows)

        # Service line acuity
        sl_data: dict[str, dict] = {}
        total_weight = 0.0
        for r in rows:
            sl = service_lines.map_drg_to_service_line(r["drg_code"])
            weight = service_lines.get_drg_weight(r["drg_code"])
            contrib = weight * r["discharges"]
            total_weight += contrib

            if sl not in sl_data:
                sl_data[sl] = {"discharges": 0, "weighted_sum": 0.0}
            sl_data[sl]["discharges"] += r["discharges"]
            sl_data[sl]["weighted_sum"] += contrib

        sl_acuity = []
        for sl, d in sorted(sl_data.items(), key=lambda x: x[1]["weighted_sum"], reverse=True):
            sl_acuity.append(ServiceLineAcuity(
                service_line=sl,
                discharges=d["discharges"],
                avg_drg_weight=round(d["weighted_sum"] / d["discharges"], 4) if d["discharges"] else 0,
                pct_of_total_weight=round(d["weighted_sum"] / total_weight * 100, 1) if total_weight else 0,
            ))

        # Top DRGs by weight contribution
        drg_contribs = []
        for r in rows:
            weight = service_lines.get_drg_weight(r["drg_code"])
            contrib = weight * r["discharges"]
            drg_contribs.append(DRGWeightContribution(
                drg_code=r["drg_code"],
                drg_description=r["drg_desc"],
                service_line=service_lines.map_drg_to_service_line(r["drg_code"]),
                discharges=r["discharges"],
                drg_weight=weight,
                total_weight_contribution=round(contrib, 2),
                pct_of_total_weight=round(contrib / total_weight * 100, 1) if total_weight else 0,
            ))

        response = CaseMixResponse(
            ccn=ccn,
            provider_name=rows[0]["provider_name"] if rows else "",
            year=yr,
            case_mix_index=cmi,
            total_discharges=total_discharges,
            service_line_acuity=sl_acuity,
            top_drgs_by_weight=sorted(drg_contribs, key=lambda d: d.total_weight_contribution, reverse=True)[:25],
        )
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("compute_case_mix failed")
        return json.dumps({"error": f"compute_case_mix failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: analyze_market_volumes
# ---------------------------------------------------------------------------
@mcp.tool()
async def analyze_market_volumes(
    provider_ccns: list[str], service_line: str = "", year: str = "",
) -> str:
    """Analyze service-line market share among a set of providers.

    Compare inpatient volumes across providers within a defined market area.
    Use with service-area or geo-demographics tools to identify competitor CCNs.

    Args:
        provider_ccns: List of CCNs for providers in the market (e.g. ["390223", "390111"]).
        service_line: Filter to one service line.
        year: Discharge year. Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccns=provider_ccns)
        if not rows:
            return json.dumps({"error": "No inpatient data found for the provided CCNs"})

        # Map service lines
        for r in rows:
            r["service_line"] = service_lines.map_drg_to_service_line(r["drg_code"])

        # Apply service line filter
        if service_line:
            rows = [r for r in rows if r["service_line"].lower() == service_line.lower()]

        # Aggregate by provider
        provider_data: dict[str, dict] = {}
        for r in rows:
            ccn = r["ccn"]
            if ccn not in provider_data:
                provider_data[ccn] = {
                    "provider_name": r["provider_name"],
                    "state": r["state"],
                    "total_discharges": 0,
                    "by_sl": {},
                }
            provider_data[ccn]["total_discharges"] += r["discharges"]
            sl = r["service_line"]
            if sl not in provider_data[ccn]["by_sl"]:
                provider_data[ccn]["by_sl"][sl] = 0
            provider_data[ccn]["by_sl"][sl] += r["discharges"]

        total_market = sum(p["total_discharges"] for p in provider_data.values())

        # Market totals by service line
        sl_market: dict[str, dict] = {}
        for ccn, p in provider_data.items():
            for sl, vol in p["by_sl"].items():
                if sl not in sl_market:
                    sl_market[sl] = {"total": 0, "top_ccn": "", "top_name": "", "top_vol": 0}
                sl_market[sl]["total"] += vol
                if vol > sl_market[sl]["top_vol"]:
                    sl_market[sl]["top_ccn"] = ccn
                    sl_market[sl]["top_name"] = p["provider_name"]
                    sl_market[sl]["top_vol"] = vol

        # Build provider shares
        provider_shares = []
        for ccn, p in sorted(provider_data.items(), key=lambda x: x[1]["total_discharges"], reverse=True):
            sl_breakdown = []
            for sl, vol in sorted(p["by_sl"].items(), key=lambda x: x[1], reverse=True):
                sl_total = sl_market[sl]["total"]
                sl_breakdown.append(ServiceLineShare(
                    service_line=sl,
                    discharges=vol,
                    market_share_pct=round(vol / sl_total * 100, 1) if sl_total else 0,
                ))

            provider_shares.append(ProviderMarketShare(
                ccn=ccn,
                provider_name=p["provider_name"],
                state=p["state"],
                total_discharges=p["total_discharges"],
                market_share_pct=round(p["total_discharges"] / total_market * 100, 1) if total_market else 0,
                service_line_breakdown=sl_breakdown,
            ))

        sl_totals = [
            ServiceLineMarketTotal(
                service_line=sl,
                total_discharges=d["total"],
                pct_of_market=round(d["total"] / total_market * 100, 1) if total_market else 0,
                top_provider_ccn=d["top_ccn"],
                top_provider_name=d["top_name"],
            )
            for sl, d in sorted(sl_market.items(), key=lambda x: x[1]["total"], reverse=True)
        ]

        response = MarketVolumesResponse(
            year=yr,
            total_market_discharges=total_market,
            total_providers=len(provider_data),
            provider_shares=provider_shares,
            service_line_totals=sl_totals,
        )
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("analyze_market_volumes failed")
        return json.dumps({"error": f"analyze_market_volumes failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
```

**Step 2: Verify server imports**

```bash
python -c "from servers.claims_analytics.server import mcp; print(f'Server: {mcp.name}, tools: OK')"
```

Expected: `Server: claims-analytics, tools: OK`

**Step 3: Commit**

```bash
git add servers/claims_analytics/server.py
git commit -m "feat(claims-analytics): add FastMCP server with 5 tools"
```

---

### Task 7: Add Docker and MCP registration

**Files:**
- Modify: `docker-compose.yml` (add claims-analytics service)
- Modify: `.mcp.json` (add claims-analytics entry)

**Step 1: Add to docker-compose.yml**

Append before the `volumes:` section:

```yaml
  claims-analytics:
    build: .
    command: python -m servers.claims_analytics.server
    ports:
      - "8012:8012"
    environment:
      - MCP_TRANSPORT=streamable-http
      - MCP_PORT=8012
    volumes:
      - healthcare-cache:/root/.healthcare-data-mcp/cache
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8012),5); s.close()"]
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 30s
```

**Step 2: Add to .mcp.json**

Add inside `mcpServers`:

```json
"claims-analytics": {
  "type": "http",
  "url": "http://localhost:8012/mcp"
}
```

**Step 3: Commit**

```bash
git add docker-compose.yml .mcp.json
git commit -m "feat(claims-analytics): add Docker and MCP registration (port 8012)"
```

---

### Task 8: Validate server starts and tools register

**Step 1: Test server startup (stdio mode)**

```bash
cd /mnt/d/Coding\ Projects/healthcare-data-mcp
timeout 10 python -m servers.claims_analytics.server 2>&1 || true
```

Expected: Server starts without import errors. May hang waiting for stdio input — that's fine.

**Step 2: Verify all tool names are registered**

```bash
python -c "
from servers.claims_analytics.server import mcp
tools = mcp._tool_manager._tools if hasattr(mcp, '_tool_manager') else {}
print(f'Registered {len(tools)} tools:')
for name in sorted(tools):
    print(f'  - {name}')
"
```

Expected: 5 tools listed: `get_inpatient_volumes`, `get_outpatient_volumes`, `trend_service_lines`, `compute_case_mix`, `analyze_market_volumes`.

**Step 3: Verify model serialization**

```bash
python -c "
from servers.claims_analytics.models import *
import json
r = InpatientVolumesResponse(ccn='390223', provider_name='Test', total_discharges=100)
print(json.dumps(r.model_dump(), indent=2)[:200])
print('Models OK')
"
```

Expected: JSON output and `Models OK`.

---

### Task 9: Fix any issues and final commit

**Step 1: Run Pyright type checking (if available)**

```bash
cd /mnt/d/Coding\ Projects/healthcare-data-mcp
python -m pyright servers/claims_analytics/ 2>&1 | head -30 || echo "Pyright not installed, skipping"
```

Fix any type errors found.

**Step 2: Verify the complete server directory**

```bash
find servers/claims_analytics -type f | sort
ls -la servers/claims-analytics
```

Expected:
```
servers/claims_analytics/__init__.py
servers/claims_analytics/data/drg_service_line_map.csv
servers/claims_analytics/data/drg_weights_fy2024.csv
servers/claims_analytics/data_loaders.py
servers/claims_analytics/models.py
servers/claims_analytics/server.py
servers/claims_analytics/service_lines.py
```
And `servers/claims-analytics` is a symlink to `claims_analytics`.

**Step 3: Final commit if any fixes were needed**

```bash
git add -A servers/claims_analytics/
git commit -m "fix(claims-analytics): resolve type/lint issues"
```
