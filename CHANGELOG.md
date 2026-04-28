# Changelog

## Unreleased

## v0.1.1 - Development readiness cleanup

- Consolidated the open release-prep branches into a single candidate branch.
- Added release metadata, community docs, and baseline CI automation.
- Corrected shipped configuration templates so `financial-intelligence` no
  longer pretends to run without a real `SEC_USER_AGENT`.
- Cleaned public-release artifacts and removed extracted planning/prototype
  material from the packaged repository.
- Added development-readiness support for Jefferson reporting workflows,
  source-evidence validation, public cache acquisition, and bounded financial
  disclosure parsing.
- Fixed MRF registry CCNs for Jefferson/LVHN facilities so price-transparency
  entries match the canonical facility ledger.
