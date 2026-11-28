package zmuxquic

import (
	"context"

	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// A joined QUIC stream declares the Bound contract: Bound keeps lanes, and Sealed has none.
func TestQUICJoinModes(t *testing.T) {
	for _, tc := range []struct {
		mode  zmux.RecordMode
		lanes bool
	}{{zmux.RecordBound, true}, {zmux.RecordSealed, false}} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			scfg := mpConfig()
			scfg.AllowRecordMode = func(_ zmux.RecordPolicyInput, m zmux.RecordMode, _ any) bool { return m == tc.mode }
			e := newEnv(t, scfg)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			p := NewConnPool(e.ln.Addr().String(), e.cli, nil, nil, carrier.PoolLimits{MaxPipesPerConnection: 1})
			defer p.Shutdown()
			first, err := p.OpenPipe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			c, err := zmux.Start(ctx, first, zmux.RoleClient, mpConfig())
			if err != nil {
				t.Fatal(err)
			}
			s, err := e.lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = c.Close()
				_ = s.Close()
				_ = c.Wait()
				_ = s.Wait()
			}()
			second, err := p.OpenPipe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			joined, err := c.AddPipe(ctx, second, zmux.PipeOptions{RecordMode: zmux.Set(tc.mode)})
			if err != nil {
				t.Fatal(err)
			}
			if err := joined.WaitAttached(ctx); err != nil {
				t.Fatal(err)
			}
			// The traffic goes to the joined pipe, which then wants lanes of its own.
			if err := c.Pipes()[0].Update(zmux.PipeUpdate{SendStandby: zmux.Set(true)}); err != nil {
				t.Fatal(err)
			}
			echo(t, c, s, 1<<20)
			if n := len(c.PipeStats()); n != 2 {
				t.Fatalf("%d pipes", n)
			}
			lanes := func() (n uint64) {
				for _, ps := range c.PipeStats() {
					if ps.PipeID != 0 {
						n = max(n, ps.LanesOut)
					}
				}
				return n
			}
			if !tc.lanes && lanes() > 1 {
				t.Fatalf("a Sealed pipe has %d lanes", lanes())
			}
			for tc.lanes && lanes() <= 1 {
				if ctx.Err() != nil {
					t.Fatal("a Bound pipe opened no lane")
				}
				echo(t, c, s, 1<<20)
			}
		})
	}
}
