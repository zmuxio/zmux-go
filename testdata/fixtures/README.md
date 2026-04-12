# Fixture Harness Mapping

This directory vendors the upstream `zmux-spec` fixture bundle used by the
local conformance and regression tests. This note is a repository-local mapping
for maintainers so fixture coverage does not stay implicit across many test
files.

## Source files

- `wire_valid.ndjson`
    - Loaded by `loadWireFixtures(...)` in `fixtures_test.go`
    - Exercised by `TestWireValidFixtures`
    - Parsed/asserted through `assertPrefaceFixture` and `assertFrameFixture`
- `wire_invalid.ndjson`
    - Loaded by `loadWireFixtures(...)` in `fixtures_test.go`
    - Exercised by `TestWireInvalidFixtures`
    - Invalid-frame subsets are additionally mapped by `invalid_frame_fixtures_test.go`
      and `invalid_frame_fixtures_cases_test.go`
- `state_cases.ndjson`
    - Loaded by `loadStateFixtures(...)` in `state_fixtures_test.go`
    - Runtime fixture environment is created by `newStateFixtureEnv(...)` in
      `state_fixtures_setup_test.go`
    - Step execution/assertions live in `state_fixtures_steps_test.go`
- `invalid_cases.ndjson`
    - Loaded by `loadInvalidFixtures(...)` in `invalid_fixtures_test.go`
    - Supported invalid-policy/security slices are selected through
      `supportedInvalidFixtureIDs`
- `case_sets.json`
    - Loaded by `loadCaseSets(...)` in `fixture_case_sets_test.go`
    - Used to ensure local supported fixture IDs stay aligned with upstream case
      set buckets
- `index.json`
    - Loaded by `loadFixtureIndex(...)` in `fixture_case_sets_test.go`
    - Used as a shape/count sanity check for the vendored fixture bundle

## Local harness contracts

- Wire fixtures assert the public wire codec surface only; they do not depend on
  runtime session state.
- State fixtures seed one live `Conn` plus one target `Stream` through
  `seedStateFixtureStream(...)`. Fixture-style terminal seeding now writes
  explicit `send_half` / `recv_half` plus matching terminal metadata directly,
  so the harness mirrors the production decision surface.
- Invalid fixtures intentionally split by enforcement layer:
    - establishment/preface handling in `invalid_fixtures_test.go`
    - frame-shape and wrong-side stream control handling in
      `invalid_frame_fixtures_test.go`
    - case-set coverage sanity in `fixture_case_sets_test.go`

## Maintenance rule

When adding support for a new upstream fixture or case-set bucket:

1. Extend the relevant `supported*FixtureIDs` selector.
2. Add or update the concrete assertion path in the matching local test file.
3. Compare the vendored files in this directory against the upstream
   `zmux-spec` fixture bundle when intentionally refreshing fixtures, then run
   the full local Go suite to confirm the updated bundle still matches the
   repository harness.
4. Keep this mapping note in sync if the harness entry point changes.

This file is intentionally local documentation, not a second protocol source of
truth. The upstream `zmux-spec` repository remains authoritative for fixture
schema and expected behavior.
