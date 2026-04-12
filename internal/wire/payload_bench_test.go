package wire

import "testing"

func BenchmarkParseDataPayloadViewOpenMetadata(b *testing.B) {
	raw, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata|CapabilityPriorityHints|CapabilityStreamGroups, uint64PtrForPayloadBench(7), uint64PtrForPayloadBench(9), []byte("ssh"), 1024)
	if err != nil {
		b.Fatalf("BuildOpenMetadataPrefix: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		if _, err := ParseDataPayloadView(raw, FrameFlagOpenMetadata); err != nil {
			b.Fatalf("ParseDataPayloadView: %v", err)
		}
	}
}

func BenchmarkParsePriorityUpdatePayload(b *testing.B) {
	raw, err := BuildPriorityUpdatePayload(CapabilityPriorityUpdate|CapabilityPriorityHints|CapabilityStreamGroups, uint64PtrForPayloadBench(7), uint64PtrForPayloadBench(9), 1024)
	if err != nil {
		b.Fatalf("BuildPriorityUpdatePayload: %v", err)
	}
	subtype, n, err := ParseVarint(raw)
	if err != nil {
		b.Fatalf("ParseVarint: %v", err)
	}
	if EXTSubtype(subtype) != EXTPriorityUpdate {
		b.Fatalf("subtype = %v, want %v", subtype, EXTPriorityUpdate)
	}
	payload := raw[n:]

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, _, err := ParsePriorityUpdatePayload(payload); err != nil {
			b.Fatalf("ParsePriorityUpdatePayload: %v", err)
		}
	}
}

func BenchmarkAppendOpenMetadataPrefixReuse(b *testing.B) {
	var dst []byte
	priority := uint64(7)
	group := uint64(9)
	openInfo := []byte("ssh")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = AppendOpenMetadataPrefix(dst[:0], CapabilityOpenMetadata|CapabilityPriorityHints|CapabilityStreamGroups, &priority, &group, openInfo, 1024)
		if err != nil {
			b.Fatalf("AppendOpenMetadataPrefix: %v", err)
		}
	}
}

func BenchmarkAppendPriorityUpdatePayloadReuse(b *testing.B) {
	var dst []byte
	priority := uint64(7)
	group := uint64(9)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = AppendPriorityUpdatePayload(dst[:0], CapabilityPriorityUpdate|CapabilityPriorityHints|CapabilityStreamGroups, &priority, &group, 1024)
		if err != nil {
			b.Fatalf("AppendPriorityUpdatePayload: %v", err)
		}
	}
}

func uint64PtrForPayloadBench(v uint64) *uint64 {
	return &v
}
