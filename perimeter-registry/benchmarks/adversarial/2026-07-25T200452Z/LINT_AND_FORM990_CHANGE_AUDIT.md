# Lint and Form 990 change audit

Benchmark workspace opened at **2026-07-25T20:04:52Z**. This audit was recorded at
**2026-07-25T20:29:48Z**.

## Ruff failure-set audit

The PR branch and its parent 'codex/perimeter-registry' were both checked with the
same isolated Ruff 0.16.0 binary before remediation. Each had exactly **527
diagnostics in 163 files**; set comparison found **0 branch-only** and **0
parent-only** findings. The failure was therefore inherited, not introduced by
the perimeter benchmark.

The remediation used Ruff's safe fixes first, then reviewed rule-specific
structural fixes. It added no ignore rule, blanket noqa, configuration
exclusion, or CI bypass. Final result recorded at 2026-07-25T20:29:48Z:
'ruff check .' returned **All checks passed**. A targeted behavioral suite then
passed **250 tests**.

## Form 990-related repository change timestamps

- **2026-07-25T20:08:20.419530867Z** —
  'tests/servers/financial_intelligence/test_server.py': import ordering only.
- **2026-07-25T20:13:50.329112392Z** —
  'servers/financial-intelligence/irs990_parser.py': import ordering and
  replacement of three blind 'except Exception' handlers with the shared
  explicit recoverable-operation exception tuple.
- **2026-07-25T20:13:50.351112623Z** — 'shared/utils/errors.py': shared explicit
  exception tuple created for the repository-wide BLE001 remediation.

These are repository lint changes, not benchmark facts. They do not alter the
clean-room Form 990 evidence adapter contract, call or modify the existing IRS
990 Evidence Service, deploy code, change secrets, or change a production
runtime. The Nebraska and Kaiser fixture research used direct IRS-hosted public
archives and records the exact archive/member locator and source period in each
fixture.

## Benchmark artifact timestamp rule

All adversarial questions, gold, orders, resolver snapshots, run outputs,
scores, review, lookup timings, and result reports live below the immutable run
directory '2026-07-25T200452Z'. Each generated result file must also carry its
own UTC generation timestamp.

## Final validation addendum — 2026-07-25T21:02:21Z

The first full-suite pass detected that Ruff unsafe rule PYI061 had removed
Pydantic JSON-schema const metadata from 15 null-only contract fields. The
change was not accepted. Those fields now use explicit Field JSON-schema
metadata, preserving checked-in schemas while satisfying Ruff without an
ignore. The affected seven tests passed, then the final full suite passed
**1,015 tests with 4 skips** in 110.56 seconds. Ruff 0.16.0 remained clean.

Temporary missing local test packages (networkx, geopandas, pyarrow, build, and
twine) were installed only under /tmp/perimeter-benchmark-deps; no global or
production environment was changed.

## Benchmark/result amendment — 2026-07-25T21:16:00Z

Independent scoring completed, the exact evaluated fixture bytes were preserved under
`inputs/`, and `RESULTS.md`/`MANIFEST.json` were finalized. Review found one N4
University/Board identifier source-scope miss; the live Nebraska fixture was corrected
after preservation and the frozen strict score was retained with a truth-adjudicated
sensitivity. No further Form 990-related repository file changed after the timestamps
listed above, and the IRS 990 Evidence Service remained untouched.

## Final post-correction validation — 2026-07-25T21:21:57Z

The first post-correction full-suite invocation accidentally replaced the repository
root in `PYTHONPATH`; seven subprocess-only checks failed to import `shared` while
1,008 tests passed and 4 skipped. With the intended local environment
(`/tmp/perimeter-benchmark-deps` plus the repository root), the unchanged suite passed
**1,015 tests with 4 skips in 111.96 seconds**. This was an invocation correction, not
a test suppression or code workaround. Repository-wide Ruff and the 49 perimeter tests
remain green. No Form 990-related file changed during this validation.

## CI executable-mode follow-up — 2026-07-25T21:26:32Z

GitHub's clean checkout exposed Ruff `EXE001` for
`scripts/import_acgme_programs.py`: the file has a Python shebang and executable
permissions in the working filesystem, but Git tracked it as mode `100644`. The only
change is the tracked mode to `100755`; file content is unchanged. A Git-index audit
found no other non-executable tracked Python file with a shebang. No Form 990-related
file changed.
