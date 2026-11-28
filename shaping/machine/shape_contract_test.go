package machine

import (
	"fmt"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping"
)

func TestOutboundMachineContract(t *testing.T) {
	c, err := ParseCandidate([]byte(`{
	  "include_control":true,
	  "machines":[{"start":0,"timers":1,"states":[
	    {"record":{"pad":7},"datagram":{"pad_to":17},"next":[{"on":"datagram_plan","to":1}]},
	    {"record":{"pad":11},"enter":[{"op":"inject","after":0,"len":9}],
	     "next":[{"on":"outbound","to":2}]},
	    {"enter":[{"op":"inject","after":0,"len":5}],
	     "next":[{"on":"injection_rejected","to":1}]}]}],
	  "budget":{"wire":{"ceiling":100,"initial":20},"profile":{"extra_bytes_ppm":50000,"window_seconds":10,"floor_bytes":0,"max_added_delay":250}}
	}`), nil, SizeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	p := c.Compile(Options{})
	b := p.Policy().Budget()
	if b.Wire.Ceiling != 100 || b.Wire.Initial != 20 || b.Profile.MaxAddedDelay != 250*time.Microsecond {
		t.Fatalf("budget %+v %+v", *b.Wire, *b.Profile)
	}
	sh := p.Policy().Factory(zmux.ShapedPipe{})
	if need := shaping.CapOutbound | shaping.CapIncludeControl | shaping.CapDatagramPlan; sh.Capabilities()&need != need {
		t.Fatalf("capabilities %#x", sh.Capabilities())
	}
	var r shaping.Effects
	sh.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	var plan shaping.DatagramPlan
	sh.(shaping.DatagramPlanner).PlanDatagram(&shaping.DatagramView{}, &plan)
	if plan.PadTo != 17 || plan.Effects.Injection.ID != 0 {
		t.Fatalf("plan %+v", plan)
	}
	sh.OnEvent(&shaping.Event{Kind: shaping.EventOutbound, Data: true}, &r)
	if r.Injection.ID == 0 || r.Injection.PadBytes != 5 {
		t.Fatalf("outbound effects %+v", r)
	}
	id := r.Injection.ID
	r = shaping.Effects{}
	sh.OnEvent(&shaping.Event{Kind: shaping.EventInjectionRejected, RequestID: id}, &r)
	if r.Injection.ID != 0 {
		t.Fatalf("rejection retried immediately: %+v", r)
	}
}

func TestInvalidShapingAccounts(t *testing.T) {
	for _, raw := range []string{
		`{"wire":{"initial":1}}`,
		`{"wire":{"ceiling":0,"initial":1}}`,
		`{"profile":{"extra_bytes_ppm":1}}`,
		`{"profile":{"extra_bytes_ppm":0,"max_added_delay":-1}}`,
	} {
		if _, err := ParseLimits([]byte(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{
		`{"budget":{"wire":{"ceiling":{"t":[[1,2,3]]},"initial":{"t":[[1,1,3]]}}}}`,
		`{"machines":[{"start":0,"states":[{"next":[{"on":"record_in","to":0}]}]}]}`,
		`{"machines":[{"start":0,"states":[{"next":[{"on":"record_out","to":0}]}]}]}`,
	} {
		if _, err := ParseCandidate([]byte(raw), nil, SizeLimits{}); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestReachableRateFitsDelay(t *testing.T) {
	for _, us := range []int{0, 1999, 2000} {
		raw := fmt.Sprintf(`{"machines":[{"start":0,"states":[{"enter":[{"op":"rate","rate":32000,"burst":64},{"op":"pause","after":3000}]}]}],"budget":{"profile":{"window_seconds":3,"floor_bytes":0,"extra_bytes_ppm":0,"max_added_delay":%d}}}`, us)
		c, err := ParseCandidate([]byte(raw), nil, SizeLimits{})
		if us < 2000 {
			if limitClass(err) != "conflict" {
				t.Fatalf("%d: %v", us, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		b := c.Compile(Options{}).Policy().Budget()
		if b.RequiredGateWait != 2*time.Millisecond || b.Fits(shaping.Limits{Profile: &shaping.ProfileLimits{WindowSeconds: 1, MaxAddedDelay: 1999 * time.Microsecond}}) {
			t.Fatalf("invalid certificate %+v", b)
		}
	}
}
