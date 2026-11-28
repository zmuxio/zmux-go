package zmux

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func TestSessionCapacityPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		before, err := e.client.Capacity()
		if err != nil {
			t.Fatal(err)
		}
		if before.Envelope != 12 || before.Required != Both || before.Covered != Both ||
			before.OwnAttachments != 1 || before.LocalCarriers != 1 {
			t.Fatalf("bootstrap capacity: %+v", before)
		}
		if p, ok := e.client.Pipe(0); !ok || !errors.Is(p.Close(), ErrNoDirectionCoverage) {
			t.Fatal("planned drain removed sole coverage")
		}
		lower := SessionPolicy{Revision: before.PolicyRevision + 1, Limits: CapacityLimits{
			SteadyAttachments: 2, JoinHeadroom: new(0), LocalCarriers: 1,
		}}
		if err := e.client.SetSessionPolicy(lower); err != nil {
			t.Fatal(err)
		}
		<-before.Changed
		after, err := e.client.Capacity()
		if err != nil {
			t.Fatal(err)
		}
		if after.Observed <= before.Observed || after.Envelope != before.Envelope ||
			after.PolicyRevision != lower.Revision || after.Covered != Both || *after.Limits.JoinHeadroom != 0 {
			t.Fatalf("capacity after policy update: %+v", after)
		}
		beyond := SessionPolicy{Revision: lower.Revision + 1, Limits: before.Limits}
		beyond.Limits.SteadyAttachments++
		if err := e.client.SetSessionPolicy(beyond); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("increasing fixed envelope: %v", err)
		}
		if err := e.client.SetSessionPolicy(lower); !errors.Is(err, ErrSessionPolicyConflict) {
			t.Fatalf("reusing policy revision: %v", err)
		}
		if err := e.client.SetSessionPolicy(SessionPolicy{Revision: lower.Revision + 1, Limits: before.Limits}); err != nil {
			t.Fatalf("restoring original envelope: %v", err)
		}
	})
}

type carrierCloseGate struct {
	net.Conn
	closing chan<- struct{}
	release <-chan struct{}
	armed   atomic.Bool
}

func (c *carrierCloseGate) Close() error {
	err := c.Conn.Close()
	if c.armed.CompareAndSwap(true, false) {
		close(c.closing)
		<-c.release
	}
	return err
}

func TestCapacityRetainsCarrierThroughPhysicalClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ccfg := mpConfig()
		ccfg.SessionPolicy.Limits.LocalCarriers = 2
		e := newMemMPEnv(t, ccfg, mpConfig())
		p, err := e.pipeDialer()(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		closing, release := make(chan struct{}), make(chan struct{})
		unhold := sync.OnceFunc(func() { close(release) })
		defer unhold()
		gated := &carrierCloseGate{Conn: p.R.(net.Conn), closing: closing, release: release}
		p.R, p.W = gated, gated
		joined, err := e.client.AddPipe(t.Context(), p, PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		gated.armed.Store(true)
		ended := make(chan error, 1)
		go func() { ended <- joined.Abort() }()
		<-closing
		view, err := e.client.Capacity()
		if err != nil {
			t.Fatal(err)
		}
		if view.LocalCarriers != 2 {
			t.Fatalf("carrier released before close returned: %+v", view)
		}
		select {
		case <-joined.Done():
			t.Fatal("pipe completion was published while Close was held")
		default:
		}
		unhold()
		if err := <-ended; err != nil {
			t.Fatal(err)
		}
		<-joined.Done()
		for {
			view, err = e.client.Capacity()
			if err != nil {
				t.Fatal(err)
			}
			if view.LocalCarriers == 1 {
				break
			}
			<-view.Changed
		}
	})
}
