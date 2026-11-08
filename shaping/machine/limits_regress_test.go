package machine

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func limitClass(err error) string {
	if f, ok := errors.AsType[*FormatError](err); ok {
		return f.Class
	}
	return ""
}

func slots(c, tm int) string {
	return `{"machines":[{"start":0,"counters":` + strconv.Itoa(c) + `,"timers":` + strconv.Itoa(tm) + `,"states":[{}]}]}`
}

func TestMaxSlots(t *testing.T) {
	for _, c := range []struct {
		js   string
		l    SizeLimits
		want string
	}{
		{slots(1<<25, 1<<25), SizeLimits{}, "limit"},
		{slots(DefaultMaxSlots, 0), SizeLimits{}, ""},
		{slots(DefaultMaxSlots, 1), SizeLimits{}, "limit"},
		{slots(2, 2), SizeLimits{MaxSlots: 3}, "limit"},
		{slots(DefaultMaxSlots, 1), SizeLimits{MaxSlots: -1}, ""},
	} {
		if _, err := ParseCandidate([]byte(c.js), nil, c.l); limitClass(err) != c.want {
			t.Errorf("%s %+v: %v", c.js, c.l, err)
		}
	}
	if _, err := NewFactory([]byte(slots(1<<30, 0)), Options{}); limitClass(err) != "limit" {
		t.Errorf("NewFactory: %v", err)
	}
}

func TestMaxBytesCountsReferencedTables(t *testing.T) {
	js := []byte(`{"pool":{"count":{"t":"a"}}}`)
	tb := map[string]json.RawMessage{"a": json.RawMessage(`[[1,1,2]]`), "unused": json.RawMessage(strings.Repeat(" ", 1000))}
	if _, err := ParseCandidate(js, tb, SizeLimits{MaxBytes: len(js) + 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCandidate(js, tb, SizeLimits{MaxBytes: len(js) + 8}); limitClass(err) != "limit" {
		t.Fatalf("%v", err)
	}
}

func TestTableSharedAndSegmentsBeforeParsing(t *testing.T) {
	var segs strings.Builder
	segs.WriteString("[")
	for i := range 2000 {
		if i > 0 {
			segs.WriteString(",")
		}
		segs.WriteString("[1,1,2]")
	}
	segs.WriteString("]")
	tb := map[string]json.RawMessage{"a": json.RawMessage(segs.String())}
	refs := strings.TrimSuffix(strings.Repeat(`{"t":"a"},`, 50000), ",")
	js := []byte(`{"pool":{"dial_after":[` + refs + `]}}`)
	t0 := time.Now()
	c, err := ParseCandidate(js, tb, SizeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if d := c.pool.dialAfter; &d[0].segs[0] != &d[len(d)-1].segs[0] {
		t.Fatal("a table parsed per reference")
	}
	t.Log(time.Since(t0))
	if _, err := ParseCandidate(js, tb, SizeLimits{MaxSegments: 1999}); limitClass(err) != "limit" {
		t.Fatalf("%v", err)
	}
}

func TestOnCycle(t *testing.T) {
	got := onCycle([][]int32{{1}, {2}, {1, 3}, {3}, {}})
	want := []bool{false, true, true, true, false}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%v", got)
		}
	}
}

// A long chain of states is checked in linear time.
func TestStructureLinear(t *testing.T) {
	const n = 200000
	var b strings.Builder
	b.WriteString(`{"machines":[{"start":0,"states":[`)
	for i := range n {
		if i > 0 {
			b.WriteString(",")
		}
		if i < n-1 {
			b.WriteString(`{"next":[{"on":"enter","to":` + strconv.Itoa(i+1) + `}]}`)
		} else {
			b.WriteString(`{}`)
		}
	}
	b.WriteString(`]}]}`)
	done := make(chan error, 1)
	go func() {
		_, err := ParseCandidate([]byte(b.String()), nil, SizeLimits{})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("quadratic")
	}
}
