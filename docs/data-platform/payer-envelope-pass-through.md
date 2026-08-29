# Payer observation pass-through

P1-28 emits source-native payer observations without a producer-side
normalization artifact. The canonical pinned observation contract requires the
top-level artifact, lineage artifact reference, and activity output artifact
reference to agree, so the verified `RawArtifactStore` artifact is intentionally
the single artifact in this envelope. Candidate parsing is deterministic and
source-scoped; profile calculations and projection writes remain prohibited.
