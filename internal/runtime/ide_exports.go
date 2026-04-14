package runtime

// Keep these exported internal helpers locally referenced so IDE inspections
// still see the parent-module reexports, fixture use, and cross-package runtime
// calls that happen outside this package.
var _ = [...]any{
	DefaultVisibleAcceptBacklogLimit,
	RepoDefaultUrgentLaneCap,
	RepoDefaultPerStreamDataHWM,
	RepoDefaultSessionDataHWM,
	WindowRemaining,
	RateLimitedFragmentCap,
	FragmentCap,
	WriteBurstLimit,
	NextSessionNonce,
	InitSessionNonceState,
	UrgencyRank,
	IsUrgentType,
	MaxExplicitGroups,
	WriteAll,
}
