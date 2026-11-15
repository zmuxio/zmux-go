package zmuxhttp

import (
	"testing"

	"github.com/zmuxio/zmux-go"
)

// countChannel counts the receivers it runs.
type countChannel struct{ running int }

func (*countChannel) ChannelStreamID() (uint64, bool)  { return 0, false }
func (*countChannel) MaxChannelDatagram() int          { return 0 }
func (*countChannel) SendChannelDatagram([]byte) error { return nil }
func (c *countChannel) ReceiveChannelDatagrams(func([]byte)) func() {
	c.running++
	stopped := false
	return func() {
		if !stopped {
			stopped = true
			c.running--
		}
	}
}

type channelProvider struct{ c zmux.DatagramChannel }

func (p channelProvider) DatagramChannel() zmux.DatagramChannel { return p.c }

// The stop of a replaced registration leaves the later receiver running,
// both before the channel exists and after.
func TestChanWriterStaleStop(t *testing.T) {
	for _, early := range []bool{true, false} {
		c := &countChannel{}
		h := &httpChanWriter{&httpRespWriter{}}
		var ch zmux.DatagramChannel = c
		if !early {
			h.ch.Store(&ch)
		}
		stop1 := h.ReceiveChannelDatagrams(func([]byte) {})
		stop2 := h.ReceiveChannelDatagrams(func([]byte) {})
		stop1()
		if early {
			h.chp = channelProvider{c}
			h.openChannel()
		}
		if c.running != 1 {
			t.Fatalf("early=%v: %d receivers after a stale stop, want 1", early, c.running)
		}
		stop2()
		if c.running != 0 {
			t.Fatalf("early=%v: %d receivers after stop, want 0", early, c.running)
		}
	}
}
