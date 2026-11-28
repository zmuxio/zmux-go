package wire

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/varint"
)

type corpusEntry struct {
	Limits       map[string]uint64 `json:"limits"`
	PipeStreamID *uint64           `json:"pipe_stream_id"`
	ID           string            `json:"id"`
	Kind         string            `json:"kind"`
	Hex          string            `json:"hex"`
	Error        string            `json:"error"`
	Frame        struct {
		Data string `json:"data"`
	} `json:"frame"`
	Gates    []string `json:"gates"`
	Messages []string `json:"messages"`
	ZeroFill struct {
		Offset int `json:"offset"`
		Count  int `json:"count"`
	} `json:"zero_fill"`
	MaxRecordSize uint64 `json:"max_record_size"`
	Padding       int    `json:"padding"`
	NonMinimal    bool   `json:"non_minimal"`
}

func loadCorpus(t testing.TB, name string) []corpusEntry {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "testdata", "spec", "fixtures", "wire", name))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []corpusEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var e corpusEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func (e *corpusEntry) gates() Gates {
	var g Gates
	family := false
	for _, n := range e.Gates {
		switch n {
		case "max_datagram_frame_size":
			g.MaxDatagram = varint.MaxVarint
		case "reset_stream_at":
			g.ResetStreamAt = true
		case "zmux_datagram_family":
			family = true
		case "zmux_goaway":
			g.GoAway = true
		case "zmux_priority":
			g.Priority = true
		case "zmux_multipath":
			g.Multipath = true
		case "zmux_pipe_ack":
			g.PipeAck = true
		case "zmux_max_lanes":
			g.Lanes = true
		}
	}
	if v, ok := e.Limits["max_datagram_frame_size"]; ok {
		g.MaxDatagram = v
	}
	g.Lanes = g.Lanes && (g.Multipath || g.PipeAck)
	g.DatagramAck = family && g.MaxDatagram > 0
	return g
}

// processFrames decodes a record's frames the way a receiver does for a
// record that is not the first of a bootstrap pipe.
func processFrames(b []byte, g *Gates) *Error {
	var f Frame
	for len(b) > 0 {
		n, err := DecodeFrame(b, &f)
		if err != nil {
			return err
		}
		if f.Kind == KindTransportParams {
			var p Params
			if err := DecodeParams(f.Data, &p); err != nil {
				return err
			}
		}
		if err := CheckGate(&f, g); err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func processEntry(e *corpusEntry) *Error {
	g := e.gates()
	limit := uint64(65535)
	if e.MaxRecordSize != 0 {
		limit = e.MaxRecordSize
	}
	switch e.Kind {
	case "frame":
		b, _ := hex.DecodeString(e.Hex)
		var f Frame
		n, err := DecodeFrame(b, &f)
		if err != nil {
			return err
		}
		if n != len(b) {
			return ErrFrame("trailing bytes")
		}
		if f.Kind == KindTransportParams {
			var p Params
			if err := DecodeParams(f.Data, &p); err != nil {
				return err
			}
		}
		return CheckGate(&f, &g)
	case "record":
		b, _ := hex.DecodeString(e.Hex)
		for len(b) > 0 {
			size, n := varint.ConsumeVarint(b)
			if n == 0 {
				return ErrFrame("truncated size")
			}
			if size == 0 || size > limit {
				return ErrFrame("record size")
			}
			if uint64(len(b)-n) < size {
				return ErrFrame("truncated record")
			}
			rec := b[n : n+int(size)]
			if err := processFrames(rec, &g); err != nil {
				return err
			}
			b = b[n+int(size):]
		}
	case "messages":
		for _, m := range e.Messages {
			b, _ := hex.DecodeString(m)
			if uint64(len(b)) > limit {
				return ErrFrame("message above max_record_size")
			}
			if err := processFrames(b, &g); err != nil {
				return err
			}
		}
	case "channel_datagram":
		return e.processChannelDatagram(&g)
	default:
		return ErrFrame("unknown corpus entry kind")
	}
	return nil
}

func (e *corpusEntry) processChannelDatagram(g *Gates) *Error {
	var f Frame
	b, err := hex.DecodeString(e.Hex)
	if err != nil || e.ZeroFill.Offset < 0 || e.ZeroFill.Offset > len(b) ||
		e.ZeroFill.Count < 0 || e.ZeroFill.Count > MaxRecordSize {
		return ErrFrame("invalid channel fixture encoding")
	}
	b = slices.Insert(b, e.ZeroFill.Offset, make([]byte, e.ZeroFill.Count)...)
	if e.PipeStreamID != nil {
		id, frame, ok := SplitChannelDatagram(b)
		if !ok || id != *e.PipeStreamID {
			return ErrProto("wrong Pipe Stream ID")
		}
		b = frame
	}
	if err := DecodeChannelFrame(b, &f, slices.Contains(e.Gates, "zmux_carrier_datagram_padding")); err != nil {
		return err
	}
	if err := CheckGate(&f, g); err != nil {
		return err
	}
	// A family DATAGRAM contains Sequence, Flow and a Context ID for a stream Flow.
	if g.DatagramAck {
		_, sequenceSize := varint.ConsumeVarint(f.Data)
		flow, flowSize := varint.ConsumeVarint(f.Data[sequenceSize:])
		_, contextSize := varint.ConsumeVarint(f.Data[sequenceSize+flowSize:])
		if sequenceSize == 0 || flowSize == 0 || flow != 0 && contextSize == 0 {
			return ErrProto("truncated channel datagram envelope")
		}
	}
	if e.Error == "" && (len(b)-1-len(f.Data) != e.Padding || hex.EncodeToString(f.Data) != e.Frame.Data) {
		return ErrFrame("wrong channel datagram payload or padding")
	}
	return nil
}

func TestCorpusValid(t *testing.T) {
	for _, e := range loadCorpus(t, "valid.ndjson") {
		t.Run(e.ID, func(t *testing.T) {
			if err := processEntry(&e); err != nil {
				t.Errorf("unexpected %v", err)
			}
			if e.Kind == "frame" {
				b, _ := hex.DecodeString(e.Hex)
				checkRoundTrip(t, e.ID, b, !e.NonMinimal)
			}
		})
	}
}

func TestCorpusInvalid(t *testing.T) {
	for _, e := range loadCorpus(t, "invalid.ndjson") {
		t.Run(e.ID, func(t *testing.T) {
			err := processEntry(&e)
			if err == nil {
				t.Fatalf("accepted, want %s", e.Error)
			}
			if got := TransportErrorName(err.Code); got != e.Error {
				t.Errorf("got %s (%v), want %s", got, err, e.Error)
			}
		})
	}
}

// checkRoundTrip checks the round-trip rule: re-encoding is no longer, decodes
// to an equal frame and is stable.
func checkRoundTrip(t testing.TB, id string, b []byte, exact bool) {
	t.Helper()
	var f1, f2 Frame
	n, err := DecodeFrame(b, &f1)
	if err != nil || n != len(b) {
		return
	}
	e1 := AppendFrame(nil, &f1)
	if len(e1) > len(b) {
		t.Errorf("%s: re-encoding longer: %x -> %x", id, b, e1)
	}
	if exact && !bytes.Equal(e1, b) {
		t.Errorf("%s: re-encoding differs: %x -> %x", id, b, e1)
	}
	if _, err := DecodeFrame(e1, &f2); err != nil {
		t.Errorf("%s: re-encoding does not decode: %v", id, err)
		return
	}
	if !bytes.Equal(AppendFrame(nil, &f2), e1) {
		t.Errorf("%s: unstable re-encoding", id)
	}
}

func FuzzDecodeFrame(f *testing.F) {
	for _, n := range []string{"valid.ndjson", "invalid.ndjson"} {
		for _, e := range loadCorpus(f, n) {
			if b, err := hex.DecodeString(e.Hex); err == nil {
				f.Add(b)
			}
		}
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		checkRoundTrip(t, "fuzz", b, false)
		var fr Frame
		if _, err := DecodeFrame(b, &fr); err == nil && fr.Kind == KindTransportParams {
			var p, q Params
			if DecodeParams(fr.Data, &p) == nil {
				enc, _ := AppendParams(nil, &p)
				if err := DecodeParams(enc, &q); err != nil {
					t.Fatalf("re-encoded params fail: %v", err)
				}
			}
		}
		var pr Preamble
		_ = ParsePreamble(b, 1<<16, &pr)
	})
}
