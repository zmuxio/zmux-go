# Fixture Harness Mapping

This directory vendors the upstream `zmux-spec` fixture bundle used by local conformance and regression tests.

It is local harness documentation, not a second protocol source of truth. `zmux-spec` remains authoritative for fixture
format and expected behavior.

## Files

- `wire_valid.ndjson`
    - loaded by `loadWireFixtures(...)` in `fixtures_test.go`
    - exercised by `TestWireValidFixtures` (`ParsePreface`/`ReadPreface` or `ParseFrame`/`ReadFrame`, plus every
      `expect` and `expect.decoded` field)
    - `TestWireFixtureFieldsAreAllAsserted` fails on any field the harness does not assert
- `wire_invalid.ndjson`
    - loaded by `loadWireFixtures(...)` in `fixtures_test.go`
    - exercised by `TestWireInvalidFixtures` (codec and session frame readers)
    - `frame_invalid` cases are also replayed by a raw peer against a live session in
      `TestWireInvalidFixturesCloseLiveSession` (`fixtures_live_test.go`), which expects `CLOSE(expect_error)`
- `state_cases.ndjson`
    - loaded by `loadStateFixtures(...)`
    - executed through `newStateFixtureEnv(...)` and the state fixture step/assertion helpers
- `invalid_cases.ndjson`
    - loaded by `loadInvalidFixtures(...)`
    - filtered by `supportedInvalidFixtureIDs`
    - `TestInvalidFixturesSupportedScenarios` asserts `expected_result.error` and `expected_result.scope` exactly as
      vendored; there is no local override of upstream expectations
    - session-scoped error cases are replayed against a live session in `TestInvalidFixtureFramesCloseLiveSession`,
      and preface cases carrying `hex` against a live server in `TestInvalidPrefaceFixturesFailLiveEstablishment`
- `case_sets.json`
    - loaded by `loadCaseSets(...)`
    - used to keep local supported fixture IDs aligned with upstream case-set buckets
- `index.json`
    - loaded by `loadFixtureIndex(...)`
    - used as a shape/count sanity check for the vendored bundle

## Local Harness Rules

- wire fixtures cover codec behavior; `frame_invalid` cases are additionally checked on a live session
- a preface carrying `preface_padding` is checked against the raw `settings_len` and round-trips semantically, not
  byte-for-byte, because receivers drop the padding
- state fixtures seed one live `Conn` plus one target `Stream`
- invalid fixtures are split by enforcement layer:
    - establishment and preface handling (raw `hex` through both preface readers and negotiation, or the
      `input_shape`)
    - frame-shape handling (raw frame bytes through both frame readers)
    - stream-state, wrong-side stream-control, and flow-control handling
    - case-set coverage checks

## Maintenance

The vendored files are the upstream `zmux-spec/fixtures/*` bundle. The `.ndjson` files keep the upstream records and
key order, re-serialized one record after another with two-space indentation; `case_sets.json` and `index.json` are
byte copies.

When adding support for a new upstream fixture or case-set bucket:

1. Update the matching `supported*FixtureIDs` selector.
2. Add or adjust the matching assertion path in the local test file.
3. Refresh the vendored fixture files from `zmux-spec` when needed.
4. Run the full local Go test suite.
5. Keep this mapping in sync if the harness entry point changes.

If a vendored expectation disagrees with this implementation, fix the implementation or report the fixture upstream;
do not override the expectation locally.
