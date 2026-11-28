package zmux

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

func TestCarrierSampleMapping(t *testing.T) {
	for _, tc := range []struct {
		probe  func(CarrierCounts) bool
		source CarrierSource
		known  bool
	}{
		{source: CarrierSourceLinuxTCPInfo, known: true, probe: func(c CarrierCounts) bool { return c.TCPSegments != nil && *c.TCPSegments == (TCPSegmentCounters{}) }},
		{source: CarrierSourceDarwinTCPConnectionInfo, known: true, probe: func(c CarrierCounts) bool { return c.TCPPackets != nil && *c.TCPPackets == (TCPPacketCounters{}) }},
		{source: CarrierSourceQUICGo, known: true, probe: func(c CarrierCounts) bool { return c.QUIC != nil && *c.QUIC == (QUICCounters{}) }},
		{source: CarrierSourceWindowsSIOTCPInfo, known: false, probe: func(c CarrierCounts) bool { return c == (CarrierCounts{}) }},
		{source: CarrierSourceUnknown, known: false, probe: func(c CarrierCounts) bool { return c == (CarrierCounts{}) }},
	} {
		t.Run(string(tc.source), func(t *testing.T) {
			w := &carrierTestWriter{source: tc.source, known: tc.known}
			primeCarrierSample(w)
			first, second := outboundCarrierSample(w, true), outboundCarrierSample(w, true)
			if first.Source != tc.source || first.Direction != CarrierOutbound || !tc.probe(first.Counts) ||
				first.CarrierID == (CarrierID{}) || first.CarrierID != second.CarrierID || !first.SampledAt.Equal(second.SampledAt) {
				t.Fatalf("samples = %+v and %+v", first, second)
			}
			if first == second || tc.known && first.Counts == second.Counts {
				t.Fatal("samples share mutable storage")
			}
		})
	}
}

type carrierTestWriter struct {
	source CarrierSource
	known  bool
}

func (*carrierTestWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*carrierTestWriter) Close() error                { return nil }
func (w *carrierTestWriter) CarrierSampler() *carrierstats.Sampler {
	return carrierstats.For(w, w.source, func(w *carrierTestWriter) carrierstats.Readout {
		return carrierstats.Readout{Known: w.known}
	})
}

func BenchmarkCarrierSampleCached(b *testing.B) {
	w := &carrierTestWriter{source: CarrierSourceQUICGo, known: true}
	primeCarrierSample(w)
	b.ReportAllocs()
	for b.Loop() {
		_ = outboundCarrierSample(w, true)
	}
}

func TestPipeStatsTCPCarrier(t *testing.T) {
	a, b := tcpPair(t)
	client, server := pair(t, a, b, nil, nil)
	for _, c := range []*Conn{client, server} {
		p, ok := c.Pipe(0)
		if !ok {
			t.Fatal("bootstrap pipe absent")
		}
		st, ok := p.Stats()
		if !ok || st.CarrierSample == nil {
			t.Fatal("bootstrap carrier sample absent")
		}
		got := st.CarrierSample
		if got.Source != carrierstats.TCPSource || got.Generation != 0 || got.Direction != CarrierOutbound {
			t.Fatalf("bootstrap sample = %+v", got)
		}
		all := c.PipeStats()
		if len(all) != 1 || all[0].CarrierSample == nil || all[0].CarrierSample.CarrierID != got.CarrierID {
			t.Fatalf("bulk sample = %+v", all)
		}
	}
}

func BenchmarkTCPPairQuery(b *testing.B) {
	a, c := tcpPair(b)
	b.Cleanup(func() { _ = a.Close(); _ = c.Close() })
	tcp := carrierstats.TCPConnOf(a)
	b.ReportAllocs()
	for b.Loop() {
		_ = carrierstats.ReadTCPPair(tcp)
	}
}

func TestDetachedOutboundReturnsCachedCarrierSample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &carrierTestWriter{source: CarrierSourceQUICGo, known: true}
		primeCarrierSample(w)
		first := outboundCarrierSample(w, OutboundActive.carrierRefresh())
		time.Sleep(2 * time.Second)
		last := outboundCarrierSample(w, OutboundClosed.carrierRefresh())
		synctest.Wait()
		if last == nil || !last.SampledAt.Equal(first.SampledAt) || last.CarrierID != first.CarrierID {
			t.Fatalf("detached sample = %+v, previous = %+v", last, first)
		}
		if cached := carrierstats.Cached(w.CarrierSampler()); !cached.At.Equal(first.SampledAt) {
			t.Fatalf("detached read refreshed the cache: %+v", cached)
		}
	})
}
