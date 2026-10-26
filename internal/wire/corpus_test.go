package wire

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type corpusEntry struct {
	ID            string            `json:"id"`
	Kind          string            `json:"kind"`
	Gates         []string          `json:"gates"`
	Hex           string            `json:"hex"`
	Messages      []string          `json:"messages"`
	Error         string            `json:"error"`
	Limits        map[string]uint64 `json:"limits"`
	MaxRecordSize uint64            `json:"max_record_size"`
	NonMinimal    bool              `json:"non_minimal"`
}

func loadCorpus(t testing.TB, name string) []corpusEntry {
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
	for _, n := range e.Gates {
		switch n {
		case "max_datagram_frame_size":
			g.MaxDatagram = MaxVarint
		case "reset_stream_at":
			g.ResetStreamAt = true
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
			size, n := ReadVarint(b)
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
	}
	return nil
}

func TestCorpusValid(t *testing.T) {
	for _, e := range loadCorpus(t, "valid.ndjson") {
		if err := processEntry(&e); err != nil {
			t.Errorf("%s: unexpected %v", e.ID, err)
		}
		if e.Kind == "frame" {
			b, _ := hex.DecodeString(e.Hex)
			checkRoundTrip(t, e.ID, b, !e.NonMinimal)
		}
	}
}

func TestCorpusInvalid(t *testing.T) {
	for _, e := range loadCorpus(t, "invalid.ndjson") {
		err := processEntry(&e)
		if err == nil {
			t.Errorf("%s: accepted, want %s", e.ID, e.Error)
			continue
		}
		if got := TransportErrorName(err.Code); got != e.Error {
			t.Errorf("%s: got %s (%v), want %s", e.ID, got, err, e.Error)
		}
	}
}

// checkRoundTrip checks CONFORMANCE.md 4.1: re-encoding is no longer, decodes
// to an equal frame and is stable.
func checkRoundTrip(t testing.TB, id string, b []byte, exact bool) {
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
				enc := AppendParams(nil, &p)
				if err := DecodeParams(enc, &q); err != nil {
					t.Fatalf("re-encoded params fail: %v", err)
				}
			}
		}
		var pr Preamble
		_ = ParsePreamble(b, 1<<16, &pr)
	})
}
