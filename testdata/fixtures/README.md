# Fixture Harness Mapping

This directory vendors the upstream `zmux-spec` fixture bundle used by local conformance and regression tests.

It is local harness documentation, not a second protocol source of truth. `zmux-spec` remains authoritative for fixture
format and expected behavior.

## Files

- `wire_valid.ndjson`
    - loaded by `loadWireFixtures(...)` in `fixtures_test.go`
    - exercised by `TestWireValidFixtures`
- `wire_invalid.ndjson`
    - loaded by `loadWireFixtures(...)` in `fixtures_test.go`
    - exercised by `TestWireInvalidFixtures`
    - invalid-frame subsets are also covered by `invalid_frame_fixtures_test.go` and
      `invalid_frame_fixtures_cases_test.go`
- `state_cases.ndjson`
    - loaded by `loadStateFixtures(...)`
    - executed through `newStateFixtureEnv(...)` and the state fixture step/assertion helpers
- `invalid_cases.ndjson`
    - loaded by `loadInvalidFixtures(...)`
    - filtered by `supportedInvalidFixtureIDs`
- `case_sets.json`
    - loaded by `loadCaseSets(...)`
    - used to keep local supported fixture IDs aligned with upstream case-set buckets
- `index.json`
    - loaded by `loadFixtureIndex(...)`
    - used as a shape/count sanity check for the vendored bundle

## Local Harness Rules

- wire fixtures cover codec behavior only
- state fixtures seed one live `Conn` plus one target `Stream`
- invalid fixtures are split by enforcement layer:
    - establishment and preface handling
    - frame-shape and wrong-side stream-control handling
    - case-set coverage checks

## Maintenance

When adding support for a new upstream fixture or case-set bucket:

1. Update the matching `supported*FixtureIDs` selector.
2. Add or adjust the matching assertion path in the local test file.
3. Refresh the vendored fixture files from `zmux-spec` when needed.
4. Run the full local Go test suite.
5. Keep this mapping in sync if the harness entry point changes.
