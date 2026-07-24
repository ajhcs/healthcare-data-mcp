from __future__ import annotations

import re
from datetime import date

from .models import EntityRecord, Resolution, ScopeOption
from .store import RegistryStore, normalize_text


class DeterministicResolver:
    def __init__(self, store: RegistryStore) -> None:
        self._store = store

    def resolve(self, request: str, as_of: str | None = None) -> Resolution:
        query = normalize_text(request)
        effective_date = self._parse_as_of(as_of)

        if "form 990" in query and any(word in query for word in ("sum", "total", "add")):
            return Resolution(
                status="rejected",
                question=None,
                options=(),
                flags=("separate Form 990 filers are non-additive tax perimeters",),
                as_of=as_of,
            )
        if "debt" in query or "bond" in query or "obligated group" in query:
            return self._debt(as_of)
        if query in {"jefferson health plans", "health partners plans", "jhp", "hpp"}:
            return self._health_plans(as_of)
        if "form 990" in query:
            return self._form_990(query, as_of)
        if "thomas jefferson university hospitals" in query or query == "tjuh":
            return self._entity_resolution(self._store.entity("tjuh"), as_of)
        if "fy25" in query and "trend" in query:
            return self._trend(as_of)
        if "excluding insurance" in query:
            return self._single_perimeter("system_excluding_insurance_fy2025", as_of)
        if "enterprise" in query and "revenue" in query:
            return self._single_perimeter("enterprise_fy2025", as_of)
        if query in {"jefferson health", "jefferson health revenue"}:
            result = self._single_perimeter("system_excluding_insurance_fy2025", as_of)
            return Resolution(
                status=result.status,
                question=result.question,
                options=result.options,
                entity_ids=("jhc",),
                flags=(
                    "care-delivery intent inferred from Jefferson Health",
                    "published System amount still includes university activity",
                )
                + result.flags,
                as_of=as_of,
            )
        if query in {"jefferson", "jefferson revenue"}:
            return self._scope_menu(as_of, effective_date)
        return Resolution(
            status="needs_clarification",
            question="Which Jefferson legal entity or reporting scope do you mean?",
            options=self._menu_options(),
            flags=("no deterministic intent rule matched",),
            as_of=as_of,
        )

    def _scope_menu(self, as_of: str | None, effective_date: date | None) -> Resolution:
        before_lvhn = effective_date is not None and effective_date < date(2024, 8, 1)
        exclusions = ("LVHN",) if before_lvhn else ()
        flags = ("LVHN entered the perimeter on 2024-08-01",) if before_lvhn else ()
        return Resolution(
            status="menu",
            question="Which Jefferson reporting scope do you mean?",
            options=tuple(
                ScopeOption(item.scope_id, item.label, excludes=exclusions)
                for item in self._menu_options()
            ),
            flags=flags,
            as_of=as_of,
        )

    def _menu_options(self) -> tuple[ScopeOption, ...]:
        return tuple(
            ScopeOption(scope_id, self._store.perimeter(scope_id).label)
            for scope_id in (
                "enterprise_fy2025",
                "system_excluding_insurance_fy2025",
                "insurance_fy2025",
            )
        )

    def _single_perimeter(self, perimeter_id: str, as_of: str | None) -> Resolution:
        perimeter = self._store.perimeter(perimeter_id)
        measurement = perimeter.measurements[0]
        before_lvhn = (
            perimeter_id in {"enterprise_fy2025", "system_excluding_insurance_fy2025"}
            and as_of is not None
            and self._parse_as_of(as_of) < date(2024, 8, 1)
        )
        exclusions = perimeter.excludes + (("LVHN",) if before_lvhn else ())
        date_flags = ("LVHN entered the perimeter on 2024-08-01",) if before_lvhn else ()
        return Resolution(
            status="resolved",
            question=None,
            options=(
                ScopeOption(
                    scope_id=perimeter.perimeter_id,
                    label=perimeter.label,
                    amount_usd=None if before_lvhn else measurement.amount_usd,
                    period=None if before_lvhn else measurement.period,
                    basis=(
                        "no compatible pre-LVHN measurement in fixture"
                        if before_lvhn
                        else measurement.basis
                    ),
                    includes=tuple(
                        item for item in perimeter.includes if not (before_lvhn and "LVHN" in item)
                    ),
                    excludes=exclusions,
                    flags=perimeter.flags + date_flags,
                ),
            ),
            flags=date_flags,
            as_of=as_of,
        )

    def _health_plans(self, as_of: str | None) -> Resolution:
        result = self._single_perimeter("insurance_fy2025", as_of)
        return Resolution(
            status="resolved",
            question=None,
            options=result.options,
            entity_ids=("hpp",),
            identifiers=self._store.entity("hpp").identifier_pairs(),
            flags=("Jefferson Health Plans is a marketing/reporting component",),
            as_of=as_of,
        )

    def _entity_resolution(self, entity: EntityRecord, as_of: str | None) -> Resolution:
        flags = (
            ("individual filer; not Jefferson enterprise",)
            if entity.entity_id == "tjuh"
            else ("exact legal filer; not a consolidated Jefferson total",)
        )
        return Resolution(
            status="resolved",
            question=None,
            options=(),
            entity_ids=(entity.entity_id,),
            identifiers=entity.identifier_pairs(),
            flags=flags,
            as_of=as_of,
        )

    def _form_990(self, query: str, as_of: str | None) -> Resolution:
        entity = self._form_990_entity(query)
        year_match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", query)
        tax_period_year = int(year_match.group(1)) if year_match else None
        if entity is not None and tax_period_year is not None:
            return Resolution(
                status="resolved",
                question=None,
                options=(),
                entity_ids=(entity.entity_id,),
                identifiers=entity.identifier_pairs(),
                flags=("exact filer and tax-period year selected; no facts fetched",),
                as_of=as_of,
                tax_period_year=tax_period_year,
            )
        if entity is not None:
            return Resolution(
                status="needs_clarification",
                question="Which tax year?",
                options=(
                    ScopeOption(
                        scope_id=f"form990:{entity.entity_id}",
                        label=f"{entity.name} ({entity.identifier_pairs()[0][1]})",
                    ),
                ),
                entity_ids=(entity.entity_id,),
                identifiers=entity.identifier_pairs(),
                flags=("Form 990 requires one exact tax-period year",),
                as_of=as_of,
            )
        return Resolution(
            status="needs_clarification",
            question="Which legal filer and tax year?",
            options=tuple(
                ScopeOption(
                    scope_id=f"form990:{candidate.entity_id}",
                    label=f"{candidate.name} ({candidate.identifier_pairs()[0][1]})",
                )
                for candidate in self._store.entities
            ),
            flags=("Form 990 requires one exact EIN and tax-period year",),
            as_of=as_of,
        )

    def _form_990_entity(self, query: str) -> EntityRecord | None:
        ein_match = re.search(r"(?<!\d)(\d{2}-\d{7})(?!\d)", query)
        if ein_match:
            ein = ein_match.group(1)
            return next(
                (
                    entity
                    for entity in self._store.entities
                    if ("EIN", ein) in entity.identifier_pairs()
                ),
                None,
            )
        matches: list[tuple[int, EntityRecord]] = []
        for entity in self._store.entities:
            for alias in (entity.name, *entity.aliases):
                pattern = rf"(?<!\w){re.escape(alias.casefold())}(?!\w)"
                if re.search(pattern, query):
                    matches.append((len(alias), entity))
        return max(matches, key=lambda match: match[0])[1] if matches else None

    def _trend(self, as_of: str | None) -> Resolution:
        if as_of is not None and self._parse_as_of(as_of) < date(2024, 8, 1):
            return Resolution(
                status="needs_clarification",
                question="Which pre-LVHN source period should be added?",
                options=(
                    ScopeOption(
                        scope_id="enterprise_pre_lvhn_unavailable",
                        label="Pre-LVHN enterprise trend",
                        excludes=("LVHN",),
                        flags=("no compatible pre-LVHN measurement in fixture",),
                    ),
                ),
                flags=("LVHN entered the perimeter on 2024-08-01",),
                as_of=as_of,
            )
        perimeter = self._store.perimeter("enterprise_fy2025")
        actual = next(
            item for item in perimeter.measurements if item.measurement_id == "reported_actual"
        )
        pro_forma = next(
            item for item in perimeter.measurements if item.measurement_id == "lvhn_pro_forma"
        )
        return Resolution(
            status="menu",
            question="Use reported actual or the LVHN 12-month pro forma?",
            options=(
                ScopeOption(
                    "enterprise_fy2025",
                    "FY25 reported actual",
                    actual.amount_usd,
                    actual.period,
                    actual.basis,
                ),
                ScopeOption(
                    "enterprise_fy2025_lvhn_pro_forma",
                    "FY25 LVHN pro forma",
                    pro_forma.amount_usd,
                    pro_forma.period,
                    pro_forma.basis,
                ),
            ),
            flags=("reported actual and pro forma are not interchangeable",),
            as_of=as_of,
        )

    def _debt(self, as_of: str | None) -> Resolution:
        if as_of is not None and self._parse_as_of(as_of) < date(2024, 8, 1):
            return Resolution(
                status="needs_clarification",
                question="Which pre-August-2024 debt document should be added?",
                options=(
                    ScopeOption(
                        scope_id="obligated_group_pre_lvhn_unavailable",
                        label="Pre-LVHN debt perimeter",
                        excludes=("HPP and health-plan entities", "LVHN"),
                        flags=("no compatible pre-LVHN debt perimeter in fixture",),
                    ),
                ),
                flags=("LVHN entered the perimeter on 2024-08-01",),
                as_of=as_of,
            )
        result = self._single_perimeter("obligated_group_2025", as_of)
        return Resolution(
            status="resolved",
            question=None,
            options=result.options,
            entity_ids=("tju", "jhc", "tjuh"),
            flags=("debt perimeter; not a legal entity or GAAP consolidation",),
            as_of=as_of,
        )

    @staticmethod
    def _parse_as_of(as_of: str | None) -> date | None:
        if as_of is None:
            return None
        try:
            return date.fromisoformat(as_of)
        except ValueError as error:
            raise ValueError("as_of must be an ISO date (YYYY-MM-DD)") from error
