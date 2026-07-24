---
name: form-990-facts
description: Retrieve exact, pre-ingested IRS Form 990 facts for a known legal-filer EIN and tax-period year, including revenue, expenses, net assets, top reported executive compensation, and field-level XML provenance. Use for exact EIN/year 990 questions. Do not use for brand resolution, filer discovery, comparisons, or non-990 forms.
---

# Form 990 Exact Facts

Use the `get_form_990_facts` tool for a request that supplies both an exact
nine-digit legal-filer EIN and a four-digit tax-period end year.

## Workflow

1. If either identifier is missing, ask for it. State that natural-brand
   resolution and national filer discovery are outside this capability.
2. Call `get_form_990_facts` once with the supplied EIN and year. Do not use a
   shell command, local project files, or web research as a fallback.
3. If the status is `not_found`, say that the curated store has no loaded record
   for that exact EIN/year. Do not claim that the IRS has no filing and do not
   substitute another entity.
4. For a ready result, use only returned values. Name the legal filer, EIN,
   reporting-period end, and form type. Report total revenue, total expenses,
   end-of-year net assets, and the top reported executive with total Form 990
   Part VII columns D + E + F compensation.
5. Treat `not_reported` as missing, never as zero.

## Evidence receipt

Preserve the exact-filer/no-roll-up caveat. Include the official IRS source URL,
Object ID, XML SHA-256, and the metric key, reported label, units, and XML field
path or paths for each reported fact. Retain a blank Return ID when the IRS did
not provide one; do not rename the DLN or Object ID as a submission ID.

State that brand-wide rollups, affiliate inference, cross-system comparisons,
non-990 forms, public systems without a Form 990, and user-side ingestion exceed
this capability.
