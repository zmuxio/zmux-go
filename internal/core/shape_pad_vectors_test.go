package core

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/shaping"
)

type planJSON struct {
	PadTo       int `json:"pad_to"`
	PadMultiple int `json:"pad_multiple"`
	Pad         int `json:"pad"`
	Delay       int `json:"delay"`
}

func (p planJSON) plan() shaping.UnitPlan {
	return shaping.UnitPlan{PadTo: p.PadTo, PadMultiple: p.PadMultiple, Pad: p.Pad, Delay: time.Duration(p.Delay) * time.Microsecond}
}

func TestPadVectors(t *testing.T) {
	b, err := os.ReadFile("../../testdata/shaping/pad.json")
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		PadFor []struct {
			N      int      `json:"n"`
			Plan   planJSON `json:"plan"`
			Result int      `json:"result"`
		} `json:"pad_for"`
		Limits []struct {
			Plan   planJSON `json:"plan"`
			Limits struct {
				NoPadding bool `json:"no_padding"`
				NoPause   bool `json:"no_pause"`
				MaxDelay  int  `json:"max_delay"`
			} `json:"limits"`
			Result planJSON `json:"result"`
		} `json:"limits"`
		Item []struct {
			ValueLen *int `json:"value_len"`
			Gap      int  `json:"gap"`
			Room     int  `json:"room"`
			Added    int  `json:"added"`
		} `json:"item"`
		Raw []struct {
			Gap  int `json:"gap"`
			Room int `json:"room"`
			Fill int `json:"fill"`
		} `json:"raw_fill"`
		Unit []struct {
			ValueLen *int     `json:"value_len"`
			Filler   string   `json:"filler"`
			Plan     planJSON `json:"plan"`
			Room     int      `json:"room"`
			W        int      `json:"w"`
			Target   int      `json:"target"`
			Added    int      `json:"added"`
			Wire     int      `json:"wire"`
		} `json:"unit"`
		ReplyRoom []struct {
			Result struct {
				PaddingRoom *int `json:"padding_room"`
				FramesLimit int  `json:"frames_limit"`
			} `json:"result"`
			Mode       string `json:"mode"`
			Received   int    `json:"received"`
			Base       int    `json:"base"`
			WantFrames int    `json:"want_frames"`
		} `json:"reply_room"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	for _, c := range d.PadFor {
		p := c.Plan.plan()
		if got := shaping.PadFor(c.N, &p); got != c.Result {
			t.Errorf("pad_for %+v: %d", c, got)
		}
	}
	for _, c := range d.Limits {
		p := c.Plan.plan()
		shaping.ApplyLimits(&p, &shaping.Limits{NoPadding: c.Limits.NoPadding, NoPause: c.Limits.NoPause, MaxDelay: time.Duration(c.Limits.MaxDelay) * time.Microsecond})
		if p != c.Result.plan() {
			t.Errorf("limits %+v: %+v", c, p)
		}
	}
	for _, c := range d.Item {
		v, ok := join.ItemFor(c.Gap, c.Room)
		if ok != (c.ValueLen != nil) || ok && (v != *c.ValueLen || join.ItemSize(v) != c.Added) {
			t.Errorf("item %+v: %d %v", c, v, ok)
		}
	}
	for _, c := range d.Raw {
		if got := join.RawFill(c.Gap, c.Room); got != c.Fill {
			t.Errorf("raw_fill %+v: %d", c, got)
		}
	}
	for _, c := range d.Unit {
		p := c.Plan.plan()
		tg := shaping.PadFor(c.W, &p)
		added := 0
		if c.Filler == "raw" {
			added = join.RawFill(tg-c.W, c.Room)
		} else if v, ok := join.ItemFor(tg-c.W, c.Room); ok {
			added = join.ItemSize(v)
		}
		if tg != c.Target || added != c.Added || c.W+added != c.Wire {
			t.Errorf("unit %+v: target %d added %d", c, tg, added)
		}
	}
	for _, c := range d.ReplyRoom {
		m := join.Sealed
		switch c.Mode {
		case "trusted":
			m = join.Trusted
		case "bound":
			m = join.Bound
		}
		f, pad, bounded := replyRoom(m, c.Received, c.Base, c.WantFrames)
		if f != c.Result.FramesLimit || bounded != (c.Result.PaddingRoom != nil) || bounded && pad != *c.Result.PaddingRoom {
			t.Errorf("reply_room %+v: %d %d %v", c, f, pad, bounded)
		}
	}
}
