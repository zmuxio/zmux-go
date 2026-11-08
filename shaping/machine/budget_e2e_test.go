package machine_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/shaping/machine"
)

// An injecting machine on a small account: injections go until the account
// runs out, are dropped after that, and the machine hears of it. Padding goes on.
func TestBudgetEndToEnd(t *testing.T) {
	js := `{"machines":[{"start":0,"states":[
	  {"record":{"pad":50},"enter":[{"op":"inject","after":1000,"len":100}],
	   "next":[{"on":"injected","to":0},{"on":"inject_dropped","to":1}]},
	  {"record":{"pad":50}}]}],
	 "budget":{"wire":{"initial":250},"max_delay":500000}}`
	c, err := machine.ParseCandidate([]byte(js), nil, machine.SizeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	cc := c.Compile(machine.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, b := net.Pipe()
	done := make(chan *zmux.Conn, 1)
	go func() {
		s, err := zmux.Start(ctx, zmux.NetPipe(b), zmux.RoleServer, &zmux.Config{})
		if err != nil {
			t.Error(err)
		}
		done <- s
	}()
	cl, err := zmux.Start(ctx, zmux.NetPipe(a), zmux.RoleClient, &zmux.Config{Shaper: cc.Factory(), ShapingBudget: cc.Budget()})
	if err != nil {
		t.Fatal(err)
	}
	srv := <-done
	defer func() {
		_ = cl.Close()
		if srv != nil {
			_ = srv.Close()
		}
	}()
	var st shaping.Stats
	for ctx.Err() == nil {
		if st = cl.ShapingStats(); st.InjectDropped > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if st.InjectDropped != 1 || st.Injected == 0 || st.Injected > 3 {
		t.Fatalf("stats %+v", st)
	}
	if m := cl.ShapingMeters(); m.WireLeft > 250 || m.WireSpent == 0 {
		t.Fatalf("meters %+v", m)
	}
}
