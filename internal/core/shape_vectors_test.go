package core

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
)

type padVectors struct {
	Record []struct {
		SizeField     bool `json:"size_field"`
		WriteStart    bool `json:"write_start"`
		WriteOverhead int  `json:"write_overhead"`
		Plan          struct {
			MaxLen      int `json:"max_len"`
			PadTo       int `json:"pad_to"`
			PadMultiple int `json:"pad_multiple"`
			Pad         int `json:"pad"`
		} `json:"plan"`
		Data   int `json:"data"`
		Result struct {
			FramesMax int  `json:"frames_max"`
			Clamped   bool `json:"clamped"`
			Data      int  `json:"data"`
			Frames    int  `json:"frames"`
			Padding   int  `json:"padding"`
			Wire      int  `json:"wire"`
		} `json:"result"`
	} `json:"record"`
	FirstBatch []struct {
		JoinEarlyHello int `json:"join_early_hello"`
		Base           int `json:"base"`
		PadTo          int `json:"pad_to"`
		Result         struct {
			Limit int `json:"limit"`
		} `json:"result"`
	} `json:"first_batch"`
}

func readPadVectors(t *testing.T) *padVectors {
	t.Helper()
	b, err := os.ReadFile("../../testdata/shaping/pad.json")
	if err != nil {
		t.Fatal(err)
	}
	var v padVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Record) == 0 || len(v.FirstBatch) == 0 {
		t.Fatal("empty vector sections")
	}
	return &v
}

func TestShapeVectorsRecord(t *testing.T) {
	for i, v := range readPadVectors(t).Record {
		over := 0
		if v.WriteStart {
			over = v.WriteOverhead
		}
		plan := shaping.UnitPlan{PadTo: v.Plan.PadTo, PadMultiple: v.Plan.PadMultiple, Pad: v.Plan.Pad}
		framesMax, clamped := 0, false
		if m := v.Plan.MaxLen; m > 0 {
			framesMax = framesOf(m, v.SizeField, over)
			if framesMax < shaping.MinMaxLen {
				framesMax, clamped = shaping.MinMaxLen, true
				if plan.PadTo == m {
					plan.PadTo = wireOf(framesMax, v.SizeField, over)
				}
			}
		}
		data := v.Data
		if framesMax > 0 {
			data = min(data, framesMax)
		}
		ps := pipeShape{sizeField: v.SizeField, over: over, plan: shaping.RecordPlan{UnitPlan: plan}}
		padding := ps.padFor(data)
		frames := data + padding
		r := v.Result
		if framesMax != r.FramesMax || clamped != r.Clamped || data != r.Data || frames != r.Frames ||
			padding != r.Padding || wireOf(frames, v.SizeField, over) != r.Wire {
			t.Errorf("record %d %+v: frames_max %d, clamped %v, data %d, frames %d, padding %d, wire %d",
				i, v, framesMax, clamped, data, frames, padding, wireOf(frames, v.SizeField, over))
		}
	}
}

func TestShapeVectorsFirstBatch(t *testing.T) {
	for i, v := range readPadVectors(t).FirstBatch {
		limit := v.JoinEarlyHello
		switch {
		case limit == 0:
			limit = 8192
		case limit < 0:
			limit = 0
		}
		if got := firstBatchLimit(limit, v.PadTo, v.Base); got != v.Result.Limit {
			t.Errorf("first_batch %d %+v: limit %d", i, v, got)
		}
	}
}
