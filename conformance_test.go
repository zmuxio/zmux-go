package zmux

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

var conformanceChecklistEvidence = map[string][]string{
	"pass core wire interoperability": {
		"TestWireValidFixtures",
		"TestParsePrefaceExamples",
		"TestClientServerEstablish",
	},
	"pass invalid-input handling": {
		"TestWireInvalidFixtures",
		"TestInvalidFixturesSupportedScenarios",
		"TestDuplicatePeerCloseIgnoredBeforeParseAndBudgetAccounting",
	},
	"pass extension-tolerance behavior": {
		"TestPriorityUpdateIgnoresUnknownTLV",
		"TestOpenMetadataIgnoresUnknownMetadataTLV",
		"TestInvalidFixturesSupportedScenarios",
	},
	"satisfy zmux-wire-v1": {
		"TestWireValidFixtures",
		"TestWireInvalidFixtures",
	},
	"negotiate open_metadata": {
		"TestOpenMetadataCarriesPriorityAndGroup",
	},
	"accept valid DATA|OPEN_METADATA on first opening DATA": {
		"TestOpenMetadataCarriesPriorityAndGroup",
		"TestWireExamplePairwiseZeroLengthOpenMetadataThenLaterPayload",
	},
	"reject unnegotiated or misplaced OPEN_METADATA": {
		"TestOpenInfoRequiresOpenMetadataCapability",
		"TestInvalidFixturesSupportedScenarios",
	},
	"ignore unknown metadata TLVs": {
		"TestOpenMetadataIgnoresUnknownMetadataTLV",
	},
	"drop duplicate singleton metadata while preserving the enclosing DATA": {
		"TestInvalidFixturesSupportedScenarios",
	},
	"negotiate priority_update": {
		"TestPriorityUpdateRoundTrip",
	},
	"process stream_priority and stream_group": {
		"TestPriorityUpdateRoundTrip",
	},
	"ignore open_info inside PRIORITY_UPDATE": {
		"TestPriorityUpdateIgnoresOpenInfoTLV",
	},
	"ignore unknown advisory TLVs": {
		"TestPriorityUpdateIgnoresUnknownTLV",
	},
	"ignore duplicate singleton advisory updates as one dropped update": {
		"TestPriorityUpdateDuplicateSingletonIgnored",
	},
	"document and implement the repository-default semantic operation families from API_SEMANTICS.md, including full local close helper, graceful send-half completion, read-side stop, send-side reset, whole-stream abort, structured error surfacing, open/cancel behavior, and accept visibility rules": {
		"TestCloseReadStopsPeerWritesButPreservesReverseRead",
		"TestBidiStreamWriteReadAndCloseWrite",
		"TestCloseWithErrorPropagatesWholeStreamAbort",
		"TestStructuredErrorAfterLocalCloseWrite",
		"TestVisibleAcceptBacklogRefusesNewestStream",
		"TestProvisionalOpenHardCapFailsNewest",
	},
	"document whether the binding exposes a stream-style convenience profile, a full-control protocol surface, or both": {
		"TestStableSessionInterfacesExposeDocumentedSurface",
		"TestNativeInterfacesExposeNativeQueries",
		"TestPublicAPISymbolsRemainAvailable",
	},
	"exact API spellings are not required": {
		"TestStableSessionInterfacesExposeDocumentedSurface",
		"TestNativeInterfacesExposeNativeQueries",
		"TestPublicAPISymbolsRemainAvailable",
	},
	"satisfy the stream-adapter subset from API_SEMANTICS.md, including bidirectional/unidirectional open and accept mapping": {
		"TestBidiStreamWriteReadAndCloseWrite",
		"TestUniStreamWriteReadAndCloseWrite",
	},
	"provide one consistent convenience mapping or fuller documented control layer or both": {
		"TestBidiStreamWriteReadAndCloseWrite",
		"TestUniStreamWriteReadAndCloseWrite",
		"TestStableSessionInterfacesExposeDocumentedSurface",
	},
	"document limits/non-goals": {
		"TestSendStreamCloseIgnoresAbsentReadHalf",
		"TestRecvStreamCloseIgnoresAbsentWriteHalf",
	},
	"interoperate on explicit-role and role=auto establishment": {
		"TestNegotiateAutoRoles",
		"TestNegotiateEqualAutoNonceFails",
		"TestClientServerEstablish",
	},
	"pass core stream-lifecycle scenarios": {
		"TestStateFixturesSupportedScenarios",
		"TestWireExamplePairwiseStopSendingAbortiveConclusion",
		"TestWireExamplePairwisePeerFinThenLateData",
		"TestLateNonOpeningControlOnTerminalStreamIgnored",
		"TestPeerOpenedBidiStreamRetainsIncomingSlotUntilLocalSendHalfTerminates",
		"TestPeerOpenedUniStreamReleasesIncomingSlotAfterRecvEOF",
	},
	"pass core flow-control scenarios": {
		"TestStateFixturesSupportedScenarios",
		"TestPendingMaxDataCoalescesLatestValues",
		"TestPendingBlockedCoalescesPerOffset",
		"TestReadConsumptionBatchesReplenishmentUntilThreshold",
		"TestWireExamplePairwiseSessionBlockedThenMaxDataAdvancement",
		"TestSessionReplenishClampsAdvertisedToVarint62",
		"TestStreamReplenishClampsAdvertisedToVarint62",
		"TestSessionStandingGrowthSuppressedWhileReleasedCreditStillReflectsHighUsage",
		"TestStreamStandingGrowthSuppressedWhileReleasedCreditStillReflectsHighUsage",
		"TestSessionStandingGrowthSuppressedUnderTrackedMemoryPressure",
		"TestStreamStandingGrowthSuppressedUnderTrackedMemoryPressure",
	},
	"pass core session-lifecycle scenarios": {
		"TestStateFixturesSupportedScenarios",
		"TestCloseWithOpenStreamsSendsGoAwayBeforeClose",
		"TestGoAwayMustBeNonIncreasing",
		"TestPeerCloseAfterTransportFailureStillParsesDiagnostics",
		"TestCloseSessionReturnsAcceptUniBeforeClosedChWhenCloseFrameSendStalls",
		"TestAbortReturnsBlockedWriteFinalBeforeClosedChWhenCloseFrameSendStalls",
		"TestBlockedWriteFinalAfterTransportFailureReturnsStructuredTransportError",
		"TestKeepaliveSendsIdlePing",
		"TestOutboundTransportWriteResetsKeepaliveDeadline",
		"TestResetKeepaliveDueDesynchronizesDistinctSessions",
	},
	"satisfy zmux-v1": {
		"TestWireValidFixtures",
		"TestClientServerEstablish",
		"TestOpenMetadataCarriesPriorityAndGroup",
		"TestPriorityUpdateRoundTrip",
	},
	"satisfy every currently active same-version optional surface in this repository": {
		"TestOpenMetadataCarriesPriorityAndGroup",
		"TestPriorityUpdateRoundTrip",
	},
	"negotiate and handle open_metadata, priority_update, priority_hints, and stream_groups correctly": {
		"TestOpenMetadataCarriesPriorityAndGroup",
		"TestPriorityUpdateRoundTrip",
		"TestPriorityUpdateIgnoredWhenUnnegotiated",
	},
	"satisfy the repository-defined reference-profile claim gate": {
		"TestClientEstablishmentOnlyWritesPrefaceBeforePeerPreface",
		"TestPingWaitsForOutstandingSlot",
	},
	"preserve the documented repository-default sender, memory, liveness, API, and scheduling behavior closely enough for release claims": {
		"TestCloseReadStopsPeerWritesButPreservesReverseRead",
		"TestRapidHiddenAbortChurnTriggersProtocolClose",
		"TestStatsTrackSessionMemoryPressure",
		"TestTakePendingPriorityUpdateRequestsTransfersTrackedBytesToAdvisoryQueue",
		"TestPingWaitsForOutstandingSlot",
		"TestOutboundTransportWriteResetsKeepaliveDeadline",
		"TestResetKeepaliveDueDesynchronizesDistinctSessions",
		"TestEventSurfaceOptInLeavesFlagsUnsetWithoutHandler",
		"TestAcceptQueueNotificationCoalescesBurst",
		"TestAcceptStreamDrainsBurstAfterSingleNotification",
		"TestStopSendingWithSmallInFlightTailButLargeSuppressibleQueuedTailMayGracefullyFinish",
		"TestStopSendingWithSmallCombinedCommittedTailMayGracefullyFinish",
		"TestStopSendingWithSmallInflightTailStillGracefullyFinishesOnSlowLinkDespiteLargeQueuedTail",
		"TestStopSendingWithLargeQueuedOnlyTailStillPrefersResetOnSlowLink",
		"TestConfigStopSendingTailCapStillAllowsSmallInflightTail",
		"TestSendFinAllowsQueuedTerminalDataFINDrain",
		"TestSendFinRejectsQueuedPlainDataDrain",
		"TestPendingStreamMaxDataDeferredUntilLocalOpenCommit",
		"TestPendingStreamBlockedDeferredUntilLocalOpenCommit",
		"TestLastValidLocalStreamIDCommitsWithoutReuseThenNextOpenFails",
		"TestProjectedLocalOpenExhaustionFailsBeforeCreatingGap",
		"TestPingRejectsPayloadOverLocalLimit",
		"TestPingAcceptsPayloadAtLocalLimit",
		"TestPingRejectsPayloadOverPeerLimit",
		"TestPingAcceptsPayloadAtPeerLimit",
		"TestCollectWriteBatchInterleavesStreamsByDefault",
		"TestCollectWriteBatchOrdersSameRankUrgentStreamFramesByStreamID",
		"TestCollectWriteBatchSessionScopedOrdinaryScrubsStaleBatchBiasWithoutRetainedRealState",
		"TestCollectWriteBatchSessionScopedOrdinaryPreservesRetainedBiasWhenRealStateExists",
		"TestCollectWriteBatchMixedSessionScopedOrdinaryAndRealStreamOnlyAdvancesRealClassCounters",
		"TestWaitWriteDoesNotConsumeControlNotify",
		"TestWaitReadDoesNotConsumeControlNotify",
		"TestWriteAllRetriesPartialWrites",
		"TestWriteAllReturnsNoProgressOnZeroWrite",
	},
	"repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT": {
		"TestCloseReadStopsPeerWritesButPreservesReverseRead",
		"TestCloseReadWithZeroWindowQueuesOpeningDataBeforeStopSending",
	},
	"repository-default Close() acts as a full local close helper": {
		"TestCloseWithErrorPropagatesWholeStreamAbort",
	},
	"repository-default Close() on a unidirectional stream silently ignores the locally absent direction rather than failing solely because that half does not exist": {
		"TestSendStreamCloseIgnoresAbsentReadHalf",
		"TestRecvStreamCloseIgnoresAbsentWriteHalf",
	},
	"each exposed API surface keeps one documented primary spelling per operation family, with any extra convenience spellings documented as wrappers over the same semantic action rather than as distinct lifecycle operations": {
		"TestStableSessionInterfacesExposeDocumentedSurface",
		"TestNativeInterfacesExposeNativeQueries",
		"TestPublicAPISymbolsRemainAvailable",
	},
	"before session-ready, repository-default sender behavior emits only the local preface and a fatal establishment CLOSE, and emits none of new-stream DATA, stream-scoped control, ordinary session-scoped control, or EXT": {
		"TestClientEstablishmentOnlyWritesPrefaceBeforePeerPreface",
		"TestClientEstablishmentInvalidPeerPrefaceEmitsFatalClose",
		"TestClientEstablishmentRoleConflictEmitsFatalClose",
	},
	"repository-default sender and receiver memory rules enforce the documented hidden-state, provisional-open, and late-tail bounds": {
		"TestInvalidFixturesSupportedScenarios",
		"TestStateFixturesSupportedScenarios",
		"TestRapidHiddenAbortChurnTriggersProtocolClose",
		"TestProvisionalOpenHardCapFailsNewest",
		"TestLateDataAggregateCapAfterMultipleTerminalDirections",
		"TestQueueStreamBlockedDropsWhenControlBudgetExceeded",
	},
	"repository-default liveness rules keep at most one outstanding protocol PING and do not treat weak local signals as strong progress": {
		"TestPingWaitsForOutstandingSlot",
		"TestKeepaliveSendsIdlePing",
		"TestKeepaliveTimeoutSignalsIdleTimeoutError",
		"TestOutboundTransportWriteResetsKeepaliveDeadline",
		"TestResetKeepaliveDueDesynchronizesDistinctSessions",
	},
}

func TestConformanceChecklistEvidenceReferencesExistingRootTests(t *testing.T) {
	t.Parallel()

	rootTests := collectRootTestNames(t)
	for _, item := range conformanceChecklistItems(t) {
		evidence, ok := conformanceChecklistEvidence[item]
		if !ok {
			t.Errorf("missing conformance evidence mapping for checklist item %q", item)
			continue
		}
		if len(evidence) == 0 {
			t.Errorf("empty conformance evidence mapping for checklist item %q", item)
			continue
		}
		for _, testName := range evidence {
			if _, ok := rootTests[testName]; !ok {
				t.Errorf("checklist item %q references missing root test %s", item, testName)
			}
		}
	}
}

func TestConformanceChecklistEvidenceCoversClaimAndProfileBundles(t *testing.T) {
	t.Parallel()

	for _, claim := range KnownClaims() {
		if len(claim.AcceptanceChecklist()) == 0 {
			t.Fatalf("claim %q has empty acceptance checklist", claim)
		}
		for _, item := range claim.AcceptanceChecklist() {
			if _, ok := conformanceChecklistEvidence[item]; !ok {
				t.Fatalf("claim %q checklist item %q is missing evidence mapping", claim, item)
			}
		}
	}

	for _, profile := range KnownImplementationProfiles() {
		if len(profile.AcceptanceChecklist()) == 0 {
			t.Fatalf("profile %q has empty acceptance checklist", profile)
		}
		for _, item := range profile.AcceptanceChecklist() {
			if _, ok := conformanceChecklistEvidence[item]; !ok {
				t.Fatalf("profile %q checklist item %q is missing evidence mapping", profile, item)
			}
		}
	}

	for _, item := range ReferenceProfileClaimGate() {
		if _, ok := conformanceChecklistEvidence[item]; !ok {
			t.Fatalf("reference-profile gate item %q is missing evidence mapping", item)
		}
	}
}

func conformanceChecklistItems(t *testing.T) []string {
	t.Helper()

	seen := make(map[string]struct{})
	var out []string
	add := func(items []string) {
		for _, item := range items {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}

	for _, claim := range KnownClaims() {
		add(claim.AcceptanceChecklist())
	}
	for _, profile := range KnownImplementationProfiles() {
		add(profile.AcceptanceChecklist())
	}
	add(ReferenceProfileClaimGate())
	return out
}

func collectRootTestNames(t *testing.T) map[string]struct{} {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read root dir: %v", err)
	}

	fset := token.NewFileSet()
	tests := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, entry.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			if strings.HasPrefix(fn.Name.Name, "Test") {
				tests[fn.Name.Name] = struct{}{}
			}
		}
	}
	return tests
}

func TestConformanceSuiteValidAndKnown(t *testing.T) {
	known := KnownConformanceSuites()
	if len(known) != 14 {
		t.Fatalf("len(KnownConformanceSuites()) = %d, want 14", len(known))
	}
	if known[0] != SuiteCoreWireInteroperability || known[len(known)-1] != SuiteReferenceQualityBehaviors {
		t.Fatalf("KnownConformanceSuites() = %#v", known)
	}
	known[0] = ""
	again := KnownConformanceSuites()
	if again[0] != SuiteCoreWireInteroperability {
		t.Fatalf("KnownConformanceSuites() did not return a defensive copy: %#v", again)
	}
	if !SuiteReferenceProfileClaimGate.Valid() {
		t.Fatal("SuiteReferenceProfileClaimGate.Valid() = false, want true")
	}
	if ConformanceSuite("zmux-unknown").Valid() {
		t.Fatal("unknown ConformanceSuite unexpectedly reported valid")
	}
}

func TestClaimRequiredConformanceSuites(t *testing.T) {
	wire := ClaimWireV1.RequiredConformanceSuites()
	if len(wire) != 3 || wire[0] != SuiteCoreWireInteroperability || wire[2] != SuiteExtensionTolerance {
		t.Fatalf("ClaimWireV1.RequiredConformanceSuites() = %#v", wire)
	}

	priority := ClaimPriorityUpdate.RequiredConformanceSuites()
	if len(priority) != 4 || priority[3] != SuitePriorityUpdate {
		t.Fatalf("ClaimPriorityUpdate.RequiredConformanceSuites() = %#v", priority)
	}

	priority[0] = ""
	again := ClaimPriorityUpdate.RequiredConformanceSuites()
	if again[0] != SuiteCoreWireInteroperability {
		t.Fatalf("RequiredConformanceSuites() did not return a defensive copy: %#v", again)
	}

	if got := Claim("zmux-unknown").RequiredConformanceSuites(); got != nil {
		t.Fatalf("unknown claim required suites = %#v, want nil", got)
	}
}

func TestImplementationProfileRequiredConformanceSuites(t *testing.T) {
	v1 := ProfileV1.RequiredConformanceSuites()
	if len(v1) != 10 || v1[0] != SuiteCoreWireInteroperability || v1[9] != SuiteV1ProfileCompatibility {
		t.Fatalf("ProfileV1.RequiredConformanceSuites() = %#v", v1)
	}

	reference := ProfileReferenceV1.RequiredConformanceSuites()
	if len(reference) != 14 || reference[10] != SuiteAPISemanticsProfile || reference[13] != SuiteReferenceQualityBehaviors {
		t.Fatalf("ProfileReferenceV1.RequiredConformanceSuites() = %#v", reference)
	}

	reference[0] = ""
	again := ProfileReferenceV1.RequiredConformanceSuites()
	if again[0] != SuiteCoreWireInteroperability {
		t.Fatalf("RequiredConformanceSuites() did not return a defensive copy: %#v", again)
	}

	if got := ImplementationProfile("zmux-unknown").RequiredConformanceSuites(); got != nil {
		t.Fatalf("unknown profile required suites = %#v, want nil", got)
	}
}

func TestImplementationProfileReleaseCertificationGate(t *testing.T) {
	gate := ProfileReferenceV1.ReleaseCertificationGate()
	if len(gate) != 14 || gate[12] != SuiteReferenceProfileClaimGate {
		t.Fatalf("ProfileReferenceV1.ReleaseCertificationGate() = %#v", gate)
	}

	gate[0] = ""
	again := ProfileReferenceV1.ReleaseCertificationGate()
	if again[0] != SuiteCoreWireInteroperability {
		t.Fatalf("ReleaseCertificationGate() did not return a defensive copy: %#v", again)
	}
}

func TestCoreModuleTargetConformance(t *testing.T) {
	claims := CoreModuleTargetClaims()
	if len(claims) != 4 ||
		claims[0] != ClaimWireV1 ||
		claims[1] != ClaimAPISemanticsProfileV1 ||
		claims[2] != ClaimOpenMetadata ||
		claims[3] != ClaimPriorityUpdate {
		t.Fatalf("CoreModuleTargetClaims() = %#v", claims)
	}

	profiles := CoreModuleTargetImplementationProfiles()
	if len(profiles) != 1 || profiles[0] != ProfileV1 {
		t.Fatalf("CoreModuleTargetImplementationProfiles() = %#v", profiles)
	}

	suites := CoreModuleTargetSuites()
	if len(suites) != 11 ||
		suites[0] != SuiteCoreWireInteroperability ||
		suites[9] != SuiteV1ProfileCompatibility ||
		suites[10] != SuiteAPISemanticsProfile {
		t.Fatalf("CoreModuleTargetSuites() = %#v", suites)
	}

	claims[0] = "changed"
	profiles[0] = "changed"
	suites[0] = "changed"
	if CoreModuleTargetClaims()[0] != ClaimWireV1 ||
		CoreModuleTargetImplementationProfiles()[0] != ProfileV1 ||
		CoreModuleTargetSuites()[0] != SuiteCoreWireInteroperability {
		t.Fatal("core module target conformance helpers did not return defensive copies")
	}
}

func TestKnownClaimsReturnsCopy(t *testing.T) {
	t.Parallel()

	got := KnownClaims()
	if len(got) != 5 {
		t.Fatalf("len(KnownClaims()) = %d, want 5", len(got))
	}
	if got[0] != ClaimWireV1 || got[4] != ClaimPriorityUpdate {
		t.Fatalf("KnownClaims() = %#v", got)
	}

	got[0] = "changed"
	again := KnownClaims()
	if again[0] != ClaimWireV1 {
		t.Fatalf("KnownClaims() did not return a defensive copy: %#v", again)
	}
}

func TestKnownImplementationProfilesReturnsCopy(t *testing.T) {
	t.Parallel()

	got := KnownImplementationProfiles()
	if len(got) != 2 {
		t.Fatalf("len(KnownImplementationProfiles()) = %d, want 2", len(got))
	}
	if got[0] != ProfileV1 || got[1] != ProfileReferenceV1 {
		t.Fatalf("KnownImplementationProfiles() = %#v", got)
	}

	got[0] = "changed"
	again := KnownImplementationProfiles()
	if again[0] != ProfileV1 {
		t.Fatalf("KnownImplementationProfiles() did not return a defensive copy: %#v", again)
	}
}

func TestClaimAndProfileValidation(t *testing.T) {
	t.Parallel()

	if !ClaimWireV1.Valid() || !ClaimPriorityUpdate.Valid() {
		t.Fatal("expected known claims to validate")
	}
	if Claim("zmux-unknown").Valid() {
		t.Fatal("unexpected valid result for unknown claim")
	}

	if !ProfileV1.Valid() || !ProfileReferenceV1.Valid() {
		t.Fatal("expected known profiles to validate")
	}
	if ImplementationProfile("zmux-unknown").Valid() {
		t.Fatal("unexpected valid result for unknown profile")
	}
}

func TestImplementationProfileClaims(t *testing.T) {
	t.Parallel()

	v1 := ProfileV1.Claims()
	if len(v1) != 3 || v1[0] != ClaimWireV1 || v1[1] != ClaimOpenMetadata || v1[2] != ClaimPriorityUpdate {
		t.Fatalf("ProfileV1.Claims() = %#v", v1)
	}

	reference := ProfileReferenceV1.Claims()
	if len(reference) != 5 {
		t.Fatalf("len(ProfileReferenceV1.Claims()) = %d, want 5", len(reference))
	}
	if reference[0] != ClaimWireV1 ||
		reference[1] != ClaimAPISemanticsProfileV1 ||
		reference[2] != ClaimStreamAdapterProfileV1 ||
		reference[3] != ClaimOpenMetadata ||
		reference[4] != ClaimPriorityUpdate {
		t.Fatalf("ProfileReferenceV1.Claims() = %#v", reference)
	}

	reference[0] = "changed"
	again := ProfileReferenceV1.Claims()
	if again[0] != ClaimWireV1 {
		t.Fatalf("Claims() did not return a defensive copy: %#v", again)
	}

	if got := ImplementationProfile("zmux-unknown").Claims(); got != nil {
		t.Fatalf("unknown profile claims = %#v, want nil", got)
	}
}

func TestClaimAcceptanceChecklist(t *testing.T) {
	t.Parallel()

	wire := ClaimWireV1.AcceptanceChecklist()
	if len(wire) != 3 || wire[0] != "pass core wire interoperability" || wire[2] != "pass extension-tolerance behavior" {
		t.Fatalf("ClaimWireV1.AcceptanceChecklist() = %#v", wire)
	}

	referenceAPI := ClaimAPISemanticsProfileV1.AcceptanceChecklist()
	if len(referenceAPI) != 3 {
		t.Fatalf("len(ClaimAPISemanticsProfileV1.AcceptanceChecklist()) = %d, want 3", len(referenceAPI))
	}
	if referenceAPI[0] != "document and implement the repository-default semantic operation families from API_SEMANTICS.md, including full local close helper, graceful send-half completion, read-side stop, send-side reset, whole-stream abort, structured error surfacing, open/cancel behavior, and accept visibility rules" {
		t.Fatalf("ClaimAPISemanticsProfileV1.AcceptanceChecklist() = %#v", referenceAPI)
	}
	if referenceAPI[2] != "exact API spellings are not required" {
		t.Fatalf("ClaimAPISemanticsProfileV1.AcceptanceChecklist() = %#v", referenceAPI)
	}

	adapter := ClaimStreamAdapterProfileV1.AcceptanceChecklist()
	if len(adapter) != 3 || adapter[1] != "provide one consistent convenience mapping or fuller documented control layer or both" {
		t.Fatalf("ClaimStreamAdapterProfileV1.AcceptanceChecklist() = %#v", adapter)
	}

	wire[0] = "changed"
	again := ClaimWireV1.AcceptanceChecklist()
	if again[0] != "pass core wire interoperability" {
		t.Fatalf("AcceptanceChecklist() did not return a defensive copy: %#v", again)
	}

	if got := Claim("zmux-unknown").AcceptanceChecklist(); got != nil {
		t.Fatalf("unknown claim checklist = %#v, want nil", got)
	}
}

func TestImplementationProfileAcceptanceChecklist(t *testing.T) {
	t.Parallel()

	v1 := ProfileV1.AcceptanceChecklist()
	if len(v1) != 7 || v1[0] != "satisfy zmux-wire-v1" || v1[6] != "negotiate and handle open_metadata, priority_update, priority_hints, and stream_groups correctly" {
		t.Fatalf("ProfileV1.AcceptanceChecklist() = %#v", v1)
	}

	reference := ProfileReferenceV1.AcceptanceChecklist()
	if len(reference) != 3 {
		t.Fatalf("len(ProfileReferenceV1.AcceptanceChecklist()) = %d, want 3", len(reference))
	}
	if reference[1] != "satisfy the repository-defined reference-profile claim gate" {
		t.Fatalf("ProfileReferenceV1.AcceptanceChecklist() = %#v", reference)
	}

	reference[0] = "changed"
	again := ProfileReferenceV1.AcceptanceChecklist()
	if again[0] != "satisfy zmux-v1" {
		t.Fatalf("AcceptanceChecklist() did not return a defensive copy: %#v", again)
	}

	if got := ImplementationProfile("zmux-unknown").AcceptanceChecklist(); got != nil {
		t.Fatalf("unknown profile checklist = %#v, want nil", got)
	}
}

func TestReferenceProfileClaimGate(t *testing.T) {
	t.Parallel()

	gate := ReferenceProfileClaimGate()
	if len(gate) != 7 {
		t.Fatalf("len(ReferenceProfileClaimGate()) = %d, want 7", len(gate))
	}
	if gate[0] != "repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT" {
		t.Fatalf("ReferenceProfileClaimGate() = %#v", gate)
	}
	if gate[6] != "repository-default liveness rules keep at most one outstanding protocol PING and do not treat weak local signals as strong progress" {
		t.Fatalf("ReferenceProfileClaimGate() = %#v", gate)
	}

	gate[0] = "changed"
	again := ReferenceProfileClaimGate()
	if again[0] != "repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT" {
		t.Fatalf("ReferenceProfileClaimGate() did not return a defensive copy: %#v", again)
	}
}

func TestCapabilitiesMetadataCarriageHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		caps                    Capabilities
		wantOpenMetadata        bool
		wantPriorityUpdate      bool
		wantOpenInfo            bool
		wantPriorityOnOpen      bool
		wantGroupOnOpen         bool
		wantPriorityInUpdate    bool
		wantGroupInUpdate       bool
		wantPeerVisiblePriority bool
		wantPeerVisibleGroup    bool
	}{
		{
			name:               "none",
			caps:               0,
			wantOpenMetadata:   false,
			wantPriorityUpdate: false,
		},
		{
			name:                    "priority_hints only",
			caps:                    CapabilityPriorityHints,
			wantPeerVisiblePriority: false,
		},
		{
			name:               "open_metadata only",
			caps:               CapabilityOpenMetadata,
			wantOpenMetadata:   true,
			wantOpenInfo:       true,
			wantPriorityUpdate: false,
		},
		{
			name:                    "open metadata priority",
			caps:                    CapabilityOpenMetadata | CapabilityPriorityHints,
			wantOpenMetadata:        true,
			wantOpenInfo:            true,
			wantPriorityOnOpen:      true,
			wantPeerVisiblePriority: true,
		},
		{
			name:                 "open metadata group",
			caps:                 CapabilityOpenMetadata | CapabilityStreamGroups,
			wantOpenMetadata:     true,
			wantOpenInfo:         true,
			wantGroupOnOpen:      true,
			wantPeerVisibleGroup: true,
		},
		{
			name:                    "priority update priority",
			caps:                    CapabilityPriorityUpdate | CapabilityPriorityHints,
			wantPriorityUpdate:      true,
			wantPriorityInUpdate:    true,
			wantPeerVisiblePriority: true,
		},
		{
			name:                 "priority update group",
			caps:                 CapabilityPriorityUpdate | CapabilityStreamGroups,
			wantPriorityUpdate:   true,
			wantGroupInUpdate:    true,
			wantPeerVisibleGroup: true,
		},
		{
			name:                    "all standardized metadata carriage",
			caps:                    CapabilityOpenMetadata | CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups,
			wantOpenMetadata:        true,
			wantPriorityUpdate:      true,
			wantOpenInfo:            true,
			wantPriorityOnOpen:      true,
			wantGroupOnOpen:         true,
			wantPriorityInUpdate:    true,
			wantGroupInUpdate:       true,
			wantPeerVisiblePriority: true,
			wantPeerVisibleGroup:    true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.caps.SupportsOpenMetadataCarriage(); got != tc.wantOpenMetadata {
				t.Fatalf("SupportsOpenMetadataCarriage() = %v, want %v", got, tc.wantOpenMetadata)
			}
			if got := tc.caps.SupportsPriorityUpdateCarriage(); got != tc.wantPriorityUpdate {
				t.Fatalf("SupportsPriorityUpdateCarriage() = %v, want %v", got, tc.wantPriorityUpdate)
			}
			if got := tc.caps.CanCarryOpenInfo(); got != tc.wantOpenInfo {
				t.Fatalf("CanCarryOpenInfo() = %v, want %v", got, tc.wantOpenInfo)
			}
			if got := tc.caps.CanCarryPriorityOnOpen(); got != tc.wantPriorityOnOpen {
				t.Fatalf("CanCarryPriorityOnOpen() = %v, want %v", got, tc.wantPriorityOnOpen)
			}
			if got := tc.caps.CanCarryGroupOnOpen(); got != tc.wantGroupOnOpen {
				t.Fatalf("CanCarryGroupOnOpen() = %v, want %v", got, tc.wantGroupOnOpen)
			}
			if got := tc.caps.CanCarryPriorityInUpdate(); got != tc.wantPriorityInUpdate {
				t.Fatalf("CanCarryPriorityInUpdate() = %v, want %v", got, tc.wantPriorityInUpdate)
			}
			if got := tc.caps.CanCarryGroupInUpdate(); got != tc.wantGroupInUpdate {
				t.Fatalf("CanCarryGroupInUpdate() = %v, want %v", got, tc.wantGroupInUpdate)
			}
			if got := tc.caps.HasPeerVisiblePrioritySemantics(); got != tc.wantPeerVisiblePriority {
				t.Fatalf("HasPeerVisiblePrioritySemantics() = %v, want %v", got, tc.wantPeerVisiblePriority)
			}
			if got := tc.caps.HasPeerVisibleGroupSemantics(); got != tc.wantPeerVisibleGroup {
				t.Fatalf("HasPeerVisibleGroupSemantics() = %v, want %v", got, tc.wantPeerVisibleGroup)
			}
		})
	}
}

func TestRegistryReservedRetiredValuesRemainStable(t *testing.T) {
	t.Parallel()

	got := []Capabilities{CapabilityMultilinkBasic}[0]
	wantAlias := []Capabilities{CapabilityMultilinkBasicRetired}[0]
	if got != wantAlias {
		t.Fatalf("CapabilityMultilinkBasic = %d, want alias of %d", got, wantAlias)
	}
	if got != Capabilities(1<<2) {
		t.Fatalf("CapabilityMultilinkBasic = %d, want %d", got, 1<<2)
	}

	tests := []struct {
		name string
		got  EXTSubtype
		want EXTSubtype
	}{
		{name: "ml_ready", got: ExtMLReadyRetired, want: 2},
		{name: "ml_attach", got: ExtMLAttachRetired, want: 3},
		{name: "ml_attach_ack", got: ExtMLAttachAckRetired, want: 4},
		{name: "ml_drain_req", got: ExtMLDrainReqRetired, want: 5},
		{name: "ml_drain_ack", got: ExtMLDrainAckRetired, want: 6},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Fatalf("%s subtype = %d, want %d", tc.name, tc.got, tc.want)
			}
		})
	}
}

func TestRegistryReservedRetiredSubtypeNamesRemainStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		got  EXTSubtype
		want string
	}{
		{got: EXTPriorityUpdate, want: "PRIORITY_UPDATE"},
		{got: ExtMLReadyRetired, want: "ML_READY"},
		{got: ExtMLAttachRetired, want: "ML_ATTACH"},
		{got: ExtMLAttachAckRetired, want: "ML_ATTACH_ACK"},
		{got: ExtMLDrainReqRetired, want: "ML_DRAIN_REQ"},
		{got: ExtMLDrainAckRetired, want: "ML_DRAIN_ACK"},
		{got: EXTSubtype(99), want: "ext_subtype(99)"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := tc.got.String(); got != tc.want {
				t.Fatalf("EXT subtype name = %q, want %q", got, tc.want)
			}
		})
	}
}
