package zmux

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
)

func TestInboundJoinWithdrawalBeforeClaim(t *testing.T) {
	for _, name := range []string{"split parents", "attachment context"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, gate := make(chan struct{}), make(chan struct{})
				outcomes := make(chan error, 2)
				cfg := mpConfig()
				cfg.JoinOutcome = func(_ *Conn, _ PipeID, err error) { outcomes <- err }
				cfg.AllowRecordMode = func(RecordPolicyInput, RecordMode, any) bool {
					close(entered)
					<-gate
					return true
				}
				e := newMemMPEnv(t, mpConfig(), cfg)
				a, b := net.Pipe()
				wait := newAttachment(e.lst, false)
				wait.pipe = owned(NetPipe(b))
				first, withdrawFirst := context.WithCancel(t.Context())
				second, withdrawSecond := context.WithCancel(t.Context())
				defer withdrawFirst()
				defer withdrawSecond()
				// Keep the handshake context live to exercise cancellation
				// at the attachment before the asynchronous split interrupt.
				if name == "split parents" {
					wait.parentsEnded = func() bool { return first.Err() != nil && second.Err() != nil }
				} else {
					wait.ctx = first
				}
				handled, added := make(chan error, 1), make(chan error, 1)
				go func() {
					defer wait.release()
					handled <- e.lst.classify(t.Context(), PipeHint{}, wait)
				}()
				go func() {
					_, err := e.client.AddPipe(t.Context(), NetPipe(a), PipeOptions{})
					added <- err
				}()
				<-entered
				withdrawFirst()
				if name == "split parents" {
					withdrawSecond()
				}
				close(gate)
				if err := <-handled; err == nil {
					t.Error("withdrawn join succeeded")
				}
				if err := <-added; err == nil {
					t.Error("joiner received a successful reply")
				}
				synctest.Wait()
				select {
				case err := <-outcomes:
					if !errors.Is(err, context.Canceled) || errors.Is(err, ErrJoinRefused) {
						t.Errorf("JoinOutcome = %v", err)
					}
				default:
					t.Fatal("missing JoinOutcome")
				}
				select {
				case err := <-outcomes:
					t.Fatalf("duplicate JoinOutcome = %v", err)
				default:
				}
			})
		})
	}
}

func TestUnconfirmedPipeCloseAndTornSession(t *testing.T) {
	for _, torn := range []bool{false, true} {
		name := "graceful pipe close"
		if torn {
			name = "torn session"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := mpConfig()
				outcomes := make(chan error, 2)
				cfg.JoinOutcome = func(_ *Conn, _ PipeID, err error) { outcomes <- err }
				e := newMemMPEnv(t, cfg, mpConfig())
				gate := make(chan struct{})
				defer close(gate)
				n, err := e.dial(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				c := e.client
				p, err := c.AddPipe(t.Context(), NetPipe(holdLaterWrites(n, gate)), PipeOptions{ReturnOnReply: true})
				if err != nil {
					t.Fatal(err)
				}
				if st, ok := p.Stats(); !ok || st.PeerAttached {
					t.Fatalf("expected an unconfirmed pipe, got %+v", st)
				}
				var before uint64
				c.peek(func() { before = c.s.JoinFailures() })
				failures := c.ConnectionStats().PipeFailures

				if torn {
					synctest.Wait()
					func() {
						c.lock()
						var abort []*pipeDrv
						defer func() { c.publishEnd(abort) }()
						defer c.unlock()
						defer func() {
							if recover() == nil {
								t.Error("missing injected panic")
							}
							// Model the epilogue when the core could not
							// complete its own error shutdown.
							c.poisonSt = poisonTorn
							_ = c.s.TakeActions(nil)
							abort = c.recordEndLocked()
							if actions := c.s.TakeActions(nil); len(actions) != 0 {
								t.Errorf("actions left after session end: %v", actions)
							}
						}()
						panic("torn core")
					}()
					<-c.Done()
				} else if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				<-p.Done()
				err = <-outcomes
				if torn {
					var te *TransportError
					if !errors.As(err, &te) || te.ErrorCode != TransportInternalError {
						t.Errorf("JoinOutcome = %v, want internal transport error", err)
					}
				} else if !errors.Is(err, ErrPipeGone) {
					t.Errorf("JoinOutcome = %v, want %v", err, ErrPipeGone)
				}
				if !torn {
					c.peek(func() {
						if got := c.s.JoinFailures(); got != before {
							t.Errorf("join failures = %d, want %d", got, before)
						}
					})
					if got := c.ConnectionStats().PipeFailures; got != failures {
						t.Errorf("pipe failures = %d, want %d", got, failures)
					}
				}
				synctest.Wait()
				select {
				case err := <-outcomes:
					t.Fatalf("duplicate JoinOutcome = %v", err)
				default:
				}
			})
		})
	}
}

func TestArrivalRejectsFinishedOwner(t *testing.T) {
	l := mustNewListener(&ListenerConfig{})
	defer func() { _ = l.Close() }()
	owner := newAttachment(l, true)
	owner.release()
	a := &Arrival{l: l, first: owner}
	prepare := func(context.Context) error {
		t.Error("PrepareCarrier invoked the callback on a finished attachment")
		return nil
	}
	if err := a.PrepareCarrier(t.Context(), prepare); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("PrepareCarrier = %v", err)
	}
	if ok, err := a.decideRequest(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody), reqHead, nil); ok || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("decideRequest = %t, %v", ok, err)
	}
	if got := owner.refs.Load(); got != 0 {
		t.Errorf("finished owner refs = %d", got)
	}
}

func TestRetirementSkipsFinishedLane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		a := newAttachment(nil, false)
		a.release()
		c := e.client
		var pd *pipeDrv
		c.peek(func() {
			pd = c.pipes[0]
			pd.inLanes = map[*inboundLane]struct{}{
				{owner: a, close: func() { t.Error("closed a finished lane") }}: {},
			}
		})
		c.Abort(0, "")
		<-c.Done()
		if n := a.refs.Load(); n != 0 {
			t.Errorf("finished lane refs = %d", n)
		}
	})
}
