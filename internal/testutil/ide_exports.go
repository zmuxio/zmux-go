package testutil

// Keep test-helper exports locally referenced so IDE inspections still see the
// fixture helpers that are consumed from external test packages.
var _ = [...]any{
	LoadFixtureNDJSON[any],
	ReadFixtureJSON[map[string]any],
}
