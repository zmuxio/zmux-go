package state

// Keep these exported internal helpers locally referenced so IDE inspections
// still see the parent-module reexports and cross-package state-machine calls.
var _ = [...]any{
	ValidateLocalOpenID,
	StreamKindForLocal,
	FirstLocalStreamID,
	FirstPeerStreamID,
	NormalizeSendHalfState,
	ReadStopped,
	NormalizeRecvHalfState,
	DefaultProvisionalOpenHardCap,
	InitialLocalOpenedSendWindow,
	LateDataAbortState,
	ShouldCompactTerminal,
}
