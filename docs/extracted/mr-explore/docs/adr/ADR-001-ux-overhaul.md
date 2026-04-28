# ADR-001: UX Overhaul — From Data-Out to User-In

**Status:** Accepted
**Date:** 2026-02-13
**Decision Maker:** Cole (product owner) + Principal

## Context

MR-Explore v1.0.0 shipped with a functional but frustrating interface. The data layer is strong — Polars import, DuckDB queries, Rust BM25 search all perform well. The UI was built data-out: it exposes database concepts directly rather than guiding users through healthcare price transparency exploration.

User feedback (Cole, Feb 13):
- Interface is difficult and frustrating to use
- Wants Apple-like minimalist design language
- Hospital name field not auto-recognized on import
- Duplicate selections in filter dropdowns
- ICD-10 code names broken/confusing
- Statistical terms unexplained (variance, IQR, etc.)
- Buttons lack icons, app looks ugly and plain
- "High variance services" feature is broken

## Decision

### 1. Design Language: Shift to Clarity-First Minimalism

Replace the current Windows 11 Fluent theme with an Apple-inspired design system emphasizing:
- **Generous whitespace** and breathing room between elements
- **Typographic hierarchy** — SF Pro-style system fonts, clear size/weight distinctions
- **Subtle depth** — light shadows and layering instead of borders
- **Restrained color** — monochrome base with a single accent color for actions
- **Vibrancy** — translucent sidebars and panels where platform supports it

This is NOT a reskin. The `styles.py` design system architecture stays — only the values change. No structural refactor needed.

### 2. Data Intelligence Layer

Create a `src/data/recognition.py` module responsible for:
- **Fuzzy column mapping** — Levenshtein distance + keyword heuristics to match hospital name, procedure code, payer, and charge columns regardless of header naming conventions
- **Data normalization** — case normalization, whitespace trimming, and deduplication of payer names and settings before they reach the UI
- **ICD-10 enrichment** — code formatting (XXX.XX pattern) and description lookup from an embedded reference table

This separates data intelligence from both the import pipeline and the UI layer. The importer feeds raw data through recognition; the UI receives clean, normalized, enriched data.

### 3. User Education: Tooltip & Help System

Establish a `src/ui/help.py` module containing:
- A registry of tooltip text for every statistical term, UI control, and domain concept
- A `apply_tooltips(widget)` function that walks widget trees and applies contextual help
- Rich tooltips for statistical terms: plain-English definition + "why it matters" for healthcare context

Minimum coverage: every filter dropdown, every statistical metric label, every toolbar button, every chart axis.

### 4. Visual Polish: Icon System

Adopt a single icon set and integrate it:
- Use Qt's built-in QStyle standard icons as a baseline, supplemented by a lightweight bundled icon set
- Every toolbar button and action button gets an icon
- Filter dropdowns get category icons
- Tree items get contextual icons (hospital, folder, file)

### 5. Bug Fix: High Variance Services

The current implementation calculates `MAX(price) - MIN(price)` and labels it "variance." This is the **range**, not variance. Two fixes needed:
- Rename to "Price Range" or calculate actual statistical variance (standard deviation squared)
- Investigate the "broken" state Cole reported — likely a query or display failure on certain data

## Consequences

**Positive:**
- App becomes accessible to non-technical healthcare administrators
- Import wizard works reliably with real-world hospital files that don't follow CMS naming exactly
- Statistical features become educational rather than intimidating
- Professional appearance builds trust with healthcare industry users

**Negative:**
- Design language change touches every UI file — must be coordinated
- Fuzzy column matching adds complexity to import path — needs good test coverage
- ICD-10 reference data adds ~2MB to distribution size

**Risks:**
- Apple-like design on a Qt app requires discipline — Qt's native rendering differs from Cocoa. Must test on Windows where the app actually runs.
- Fuzzy matching can produce false positives on ambiguous columns — needs user confirmation step

## Alternatives Considered

1. **Incremental polish** — fix individual complaints without design language change. Rejected: the issues are systemic, not point fixes.
2. **Web-based rewrite** — would make Apple-like design easier but abandons the PyQt6 investment and offline capability. Rejected: desktop-first is a feature for healthcare environments.
3. **Keep Fluent, add icons/tooltips** — addresses symptoms but not the design language mismatch Cole identified. Rejected.
