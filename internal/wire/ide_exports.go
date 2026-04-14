package wire

// Keep these exported internal helpers locally referenced so IDE inspections
// still see the root-module wrappers, fixture loaders, and adapter consumers.
var _ = [...]any{
	ProtoVersion,
	CapabilityMultilinkBasic,
	SchedulerLatency,
	SchedulerBalancedFair,
	SchedulerBulkThroughput,
	ParseErrorPayload,
	AppendDebugTextTLVCapped,
	BuildGoAwayPayload,
	ParseGOAWAYPayload,
	AppendFrameHeaderTrustedCachedStreamID,
	ParseFrame,
	ReadFrame,
	NegotiatePrefaces,
	ParsePreface,
	ReadPreface,
	PackVarint,
	ReleaseReadFrameBuffer,
}
