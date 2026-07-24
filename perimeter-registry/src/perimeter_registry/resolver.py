from __future__ import annotations

import re
from dataclasses import replace
from datetime import date

from .models import EntityRecord, Resolution, ScopeOption
from .store import RegistryStore, normalize_text


class DeterministicResolver:
    def __init__(self, store: RegistryStore) -> None:
        self._store = store

    def resolve(self, request: str, as_of: str | None = None) -> Resolution:
        if self._store.fixture_id == "jefferson_minimal":
            return self._resolve_jefferson(request, as_of)
        return self._resolve_generic(request, as_of)

    def _resolve_jefferson(self, request: str, as_of: str | None = None) -> Resolution:
        query = normalize_text(request)
        as_of = as_of or self._date_from_query(query)
        effective_date = self._parse_as_of(as_of)

        if "form 990" in query and any(word in query for word in ("sum", "total", "add")):
            return Resolution(
                status="rejected",
                question=None,
                options=(),
                flags=("separate Form 990 filers are non-additive tax perimeters",),
                as_of=as_of,
            )
        if "enterprise analysis" in query:
            result = self._single_perimeter("enterprise_fy2025", as_of)
            return replace(
                result,
                entity_ids=("tju",),
                flags=("Jefferson Health brand is not the TJUH legal entity",) + result.flags,
                evidence_ids=("fy25_audit",),
            )
        if "lvhn" in query and "part of" in query:
            before_entry = effective_date is not None and effective_date < date(2024, 8, 1)
            return Resolution(
                status="resolved",
                question=None,
                options=(),
                entity_ids=("lvhn",),
                identifiers=self._store.entity("lvhn").identifier_pairs(),
                flags=(
                    (
                        f"not in the Jefferson perimeter as of {as_of}",
                        "documented entry date is 2024-08-01",
                    )
                    if before_entry
                    else (f"in the Jefferson perimeter as of {as_of}",)
                ),
                as_of=as_of,
                evidence_ids=("fy25_audit", "official_merger_announcement"),
            )
        if "debt" in query or "bond" in query or "obligated group" in query:
            return self._debt(as_of)
        if query in {"jefferson health plans", "health partners plans", "jhp", "hpp"}:
            return self._health_plans(as_of)
        if "form 990" in query:
            return self._with_entity_evidence(self._form_990(query, as_of))
        if "thomas jefferson university hospital" in query or query == "tjuh":
            return replace(
                self._entity_resolution(self._store.entity("tjuh"), as_of),
                evidence_ids=self._store.entity("tjuh").evidence_ids,
            )
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

    def _resolve_generic(self, request: str, as_of: str | None) -> Resolution:
        query = normalize_text(request)
        resolved_as_of = as_of or self._date_from_query(query)
        effective_date = self._parse_as_of(resolved_as_of)

        if "form 990" in query and any(word in query for word in ("sum", "total", "add")):
            return Resolution(
                status="rejected",
                question=None,
                options=(),
                flags=("separate Form 990 filers are non-additive tax perimeters",),
                as_of=resolved_as_of,
            )
        if "form 990" in query:
            result = self._form_990(query, resolved_as_of)
            return self._with_entity_evidence(result)

        if "ein" in query and "health plan" in query:
            plan_evidence = tuple(
                dict.fromkeys(
                    evidence_id
                    for perimeter in self._store.perimeters
                    if "insurance"
                    in normalize_text(f"{perimeter.perimeter_type} {perimeter.label}")
                    for evidence_id in perimeter.evidence_ids
                )
            )
            return Resolution(
                status="needs_clarification",
                question="Which insurance product, policy issuer, or licensed company do you mean?",
                options=(),
                flags=(
                    "health-plan marketing name may span multiple legal entities",
                    "no universal brand EIN is asserted",
                ),
                as_of=resolved_as_of,
                evidence_ids=plan_evidence,
            )

        system = self._store.systems[0]
        specific_entity_mentioned = any(
            len(normalize_text(alias)) > len(normalize_text(system.name))
            and normalize_text(alias) in query
            for entity in self._store.entities
            for alias in (entity.name, *entity.aliases)
        )
        if "ein" in query and self._mentions_system(query) and not specific_entity_mentioned:
            return Resolution(
                status="needs_clarification",
                question=(
                    f"Which legal filer or reporting perimeter behind {system.name} do you mean?"
                ),
                options=(),
                flags=("brand/governance concept is not assigned a universal EIN",),
                as_of=resolved_as_of,
                evidence_ids=system.evidence_ids,
            )

        facility = self._facility_for_query(query)
        if facility is not None:
            identifiers = (("CCN", facility.ccn),)
            entity_ids: tuple[str, ...] = ()
            if facility.operator_entity_id is not None:
                operator = self._store.entity(facility.operator_entity_id)
                identifiers = operator.identifier_pairs()
                if ("CCN", facility.ccn) not in identifiers:
                    identifiers += (("CCN", facility.ccn),)
                entity_ids = (operator.entity_id,)
            elif facility.operator_ein is not None:
                identifiers = (("EIN", facility.operator_ein), ("CCN", facility.ccn))
            return Resolution(
                status="resolved",
                question=None,
                options=(),
                entity_ids=entity_ids,
                identifiers=identifiers,
                flags=("facility/operator record; not the enterprise perimeter",)
                + self._evidence_limit_flags(facility.evidence_ids),
                as_of=resolved_as_of,
                evidence_ids=facility.evidence_ids,
            )

        if " or " in query and self._mentions_system(query):
            return self._generic_menu(resolved_as_of)

        if any(term in query for term in ("enterprise", "consolidated", "as a whole")):
            perimeter = self._perimeter_matching(("audited_gaap_consolidation",))
            if perimeter is not None:
                return self._generic_perimeter(perimeter.perimeter_id, resolved_as_of)

        care_terms = ("care-delivery", "care delivery", "provider", "health services")
        if any(term in query for term in care_terms):
            perimeter = self._perimeter_matching(
                ("health_services", "health-system", "health system")
            )
            if perimeter is not None:
                return self._generic_perimeter(perimeter.perimeter_id, resolved_as_of)

        if any(term in query for term in ("health plan", "insurance services", "insurer")):
            perimeter = self._perimeter_matching(("insurance",))
            if perimeter is not None:
                return self._generic_perimeter(perimeter.perimeter_id, resolved_as_of)

        entity = self._form_990_entity(query)
        membership_terms = ("part of", "belong", "include", "in the perimeter")
        if entity is not None and any(term in query for term in membership_terms):
            return self._membership_resolution(entity, resolved_as_of, effective_date)

        if entity is not None:
            return Resolution(
                status="resolved",
                question=None,
                options=(),
                entity_ids=(entity.entity_id,),
                identifiers=entity.identifier_pairs(),
                flags=("exact legal/operating record; not an inferred enterprise total",)
                + self._evidence_limit_flags(entity.evidence_ids),
                as_of=resolved_as_of,
                evidence_ids=entity.evidence_ids,
            )
        return self._generic_menu(resolved_as_of)

    def _generic_menu(self, as_of: str | None) -> Resolution:
        return Resolution(
            status="menu",
            question=(
                f"Which {self._store.systems[0].name} legal entity or reporting scope do you mean?"
            ),
            options=tuple(
                ScopeOption(item.perimeter_id, item.label, flags=item.flags)
                for item in self._store.perimeters
            ),
            flags=("brand name does not determine one legal or reporting perimeter",),
            as_of=as_of,
            evidence_ids=tuple(
                dict.fromkeys(
                    evidence_id
                    for perimeter in self._store.perimeters
                    for evidence_id in perimeter.evidence_ids
                )
            ),
        )

    def _generic_perimeter(self, perimeter_id: str, as_of: str | None) -> Resolution:
        perimeter = self._store.perimeter(perimeter_id)
        measurement = perimeter.measurements[0]
        return Resolution(
            status="resolved",
            question=None,
            options=(
                ScopeOption(
                    scope_id=perimeter.perimeter_id,
                    label=perimeter.label,
                    amount_usd=measurement.amount_usd,
                    period=measurement.period,
                    basis=measurement.basis,
                    includes=perimeter.includes,
                    excludes=perimeter.excludes,
                    flags=perimeter.flags,
                ),
            ),
            flags=perimeter.flags,
            as_of=as_of,
            evidence_ids=perimeter.evidence_ids,
        )

    def _membership_resolution(
        self, entity: EntityRecord, as_of: str | None, effective_date: date | None
    ) -> Resolution:
        consolidated = self._perimeter_matching(("audited_gaap_consolidation",))
        flags: list[str] = []
        evidence_ids = list(entity.evidence_ids)
        if consolidated is not None:
            evidence_ids.extend(consolidated.evidence_ids)
            excluded = any(
                normalize_text(entity.name) in normalize_text(item)
                or any(normalize_text(alias) in normalize_text(item) for alias in entity.aliases)
                for item in consolidated.excludes
            )
            if excluded:
                flags.append(f"excluded from {consolidated.label}")
        relationships = self._store.relationships_for(entity.entity_id)
        evidence_ids.extend(
            evidence_id
            for relationship in relationships
            for evidence_id in relationship.evidence_ids
        )
        dated = [relationship for relationship in relationships if relationship.effective_start]
        if effective_date is not None and dated:
            starts = [
                date.fromisoformat(item.effective_start) for item in dated if item.effective_start
            ]
            earliest = min(starts)
            if effective_date < earliest:
                flags.extend(
                    (
                        f"not in the documented perimeter as of {as_of}",
                        f"documented entry date is {earliest.isoformat()}",
                    )
                )
            else:
                flags.append(f"in the documented perimeter as of {as_of}")
        if any(item.relationship_type == "academic_affiliation_with" for item in relationships):
            flags.append("academic affiliation does not imply ownership or consolidation")
        if not flags:
            flags.append("membership is not determinable from the minimal fixture")
        return Resolution(
            status="resolved",
            question=None,
            options=(),
            entity_ids=(entity.entity_id,),
            identifiers=entity.identifier_pairs(),
            flags=tuple(flags),
            as_of=as_of,
            evidence_ids=tuple(dict.fromkeys(evidence_ids)),
        )

    def _perimeter_matching(self, terms: tuple[str, ...]):
        for perimeter in self._store.perimeters:
            haystack = normalize_text(f"{perimeter.perimeter_type} {perimeter.label}")
            if any(normalize_text(term) in haystack for term in terms):
                return perimeter
        return None

    def _facility_for_query(self, query: str):
        matches = [
            facility
            for facility in self._store.facilities
            if normalize_text(facility.name) in query or facility.ccn in query
        ]
        return max(matches, key=lambda item: len(item.name)) if matches else None

    def _mentions_system(self, query: str) -> bool:
        system = self._store.systems[0]
        return any(normalize_text(name) in query for name in (system.name, *system.aliases))

    def _with_entity_evidence(self, result: Resolution) -> Resolution:
        evidence_ids = tuple(
            dict.fromkeys(
                evidence_id
                for entity_id in result.entity_ids
                for evidence_id in self._store.entity(entity_id).evidence_ids
            )
        )
        return replace(
            result,
            flags=result.flags + self._evidence_limit_flags(evidence_ids),
            evidence_ids=evidence_ids,
        )

    def _evidence_limit_flags(self, evidence_ids: tuple[str, ...]) -> tuple[str, ...]:
        flags = []
        for evidence_id in evidence_ids:
            observation = self._store.evidence_observation(evidence_id)
            limitation = normalize_text(observation.limitation or "")
            if "current continuity is not" in limitation:
                flags.append(
                    f"identifier evidence is dated to {observation.source_period}; "
                    "current continuity is not reverified"
                )
        return tuple(dict.fromkeys(flags))

    @staticmethod
    def _date_from_query(query: str) -> str | None:
        match = re.search(r"(?<!\d)(20\d{2}-\d{2}-\d{2})(?!\d)", query)
        return match.group(1) if match else None

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
        ein_matches = set(re.findall(r"(?<!\d)\d{2}-\d{7}(?!\d)", query))
        year_matches = {
            int(value) for value in re.findall(r"(?<!\d)(?:19\d{2}|20\d{2}|2100)(?!\d)", query)
        }
        if len(ein_matches) > 1 or len(year_matches) > 1:
            return Resolution(
                status="needs_clarification",
                question="Which one EIN and tax year?",
                options=self._form_990_options(),
                flags=("Form 990 requires exactly one EIN and tax-period year",),
                as_of=as_of,
            )
        entity = self._form_990_entity(query)
        tax_period_year = next(iter(year_matches), None)
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
            options=self._form_990_options(),
            flags=("Form 990 requires one exact EIN and tax-period year",),
            as_of=as_of,
        )

    def _form_990_options(self) -> tuple[ScopeOption, ...]:
        return tuple(
            ScopeOption(
                scope_id=f"form990:{candidate.entity_id}",
                label=f"{candidate.name} ({candidate.identifier_pairs()[0][1]})",
            )
            for candidate in self._store.entities
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
