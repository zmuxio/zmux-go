package zmux

import (
	"context"
	"io"
	"net"
	"slices"
	"testing"
	"time"
)

// One write followed by one read measures local end-to-end latency without
// hiding a slow receiver behind a large buffer. Latency samples are bounded.
func BenchmarkSpliceTransforms(b *testing.B) {
	for _, mode := range []string{"static", "inspect", "slice", "rewrite", "group", "mirror"} {
		b.Run(mode, func(b *testing.B) {
			const width = 1024
			payload := make([]byte, width)
			opts := &SpliceOptions{}
			hook := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
				part := u.Payload(0)
				switch mode {
				case "inspect":
					var first [1]byte
					_, err := part.CopyTo(first[:], 0)
					return SpliceDecision{}, err
				case "slice":
					v, err := part.Slice(0, part.Len())
					return SpliceDecision{Replacement: []SplicePayload{v}}, err
				case "rewrite":
					v, err := u.Alloc(part.Len())
					if err != nil {
						return SpliceDecision{}, err
					}
					_, err = v.WriteAt(payload[:part.Len()], 0)
					return SpliceDecision{Replacement: []SplicePayload{v}}, err
				default:
					n := part.Len() / 2
					left, err := part.Slice(0, n)
					if err != nil {
						return SpliceDecision{}, err
					}
					right, err := part.Slice(n, part.Len())
					return SpliceDecision{Replacement: []SplicePayload{left, right}}, err
				}
			}
			if mode == "mirror" {
				m, err := NewSpliceMirror(SpliceMirrorOptions{Items: 2, BackingBytes: 2 * width, MaxPayload: width, Every: 1, Phases: SpliceMirrorOutput})
				if err != nil {
					b.Fatal(err)
				}
				opts.MirrorAtoB = m
				defer m.Close()
			} else if mode != "static" {
				opts.HooksAtoB.Stream = []SpliceHook{hook}
			}
			a, c := net.Pipe()
			client, input := spliceTestPair(b, a, c, nil)
			a, c = net.Pipe()
			target, server := spliceTestPair(b, a, c, nil)
			s, in := openSpliceTestPair(b, client, input)
			out, r := openSpliceTestPair(b, target, server)
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()
			result := make(chan SpliceResult, 1)
			go func() { res, _ := Splice(ctx, in, out, opts); result <- res }()
			output := make([]byte, width)
			samples := make([]time.Duration, 0, min(b.N, 16384))
			b.SetBytes(width)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				start := time.Now()
				if _, err := s.Write(payload); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(r, output); err != nil {
					b.Fatal(err)
				}
				if m := opts.MirrorAtoB; m != nil {
					for len(m.events) != 0 {
						if _, err := m.Next(ctx); err != nil {
							b.Fatal(err)
						}
					}
				}
				if i%max(1, b.N/cap(samples)) == 0 && len(samples) < cap(samples) {
					samples = append(samples, time.Since(start))
				}
			}
			b.StopTimer()
			_ = s.Close()
			_ = r.Close()
			res := <-result
			if res.AtoB.End.Kind != SpliceFinished {
				b.Fatalf("unexpected end %+v", res.AtoB)
			}
			slices.Sort(samples)
			b.ReportMetric(float64(samples[len(samples)/2].Nanoseconds()), "p50-ns")
			b.ReportMetric(float64(samples[(len(samples)-1)*99/100].Nanoseconds()), "p99-ns")
			b.ReportMetric(float64(res.AtoB.PayloadCopyBytes)/float64(b.N), "payload-copy-B/op")
			b.ReportMetric(float64(res.AtoB.MirrorCopyBytes)/float64(b.N), "mirror-copy-B/op")
		})
	}
}
