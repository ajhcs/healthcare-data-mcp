# HSPR financial retrieval benchmark v2

This is the leakage-resistant successor to the quarantined July 2026 studies.
Those studies tested curated, answer-bearing identity/financial evidence against
fresh identity research; they were not valid end-to-end financial retrieval
comparisons.

The v2 benchmark compares four matched Luna arms. Every arm must retrieve the
financial result from a live authoritative source; only access to an
identity/perimeter-only HSPR packet and approved reasoning level differ. Gold
is stored locally under `.benchmark-sealed/`, ignored by Git, and must be copied
only into a scorer context that is unavailable to answer agents.

No official run is authorized until cleanup commit `6cbfc2e` is in the chosen
base, its CI passes, a registry packet passes the automated leakage audit
against the adjudicated sealed key, and the pilot has been preregistered.

The current runner freezes schedules and normalizes runtime-emitted traces but
intentionally cannot execute a trial. An access-controlled answer-context
launcher with runtime-native event timestamps remains a release blocker.
