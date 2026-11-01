package zmux

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// TestClassifyFirstBytesVectors checks the shared vectors.
func TestClassifyFirstBytesVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/firstbytes.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vectors []struct {
			Hex, Framing, Class, Note string
			Need                      int
		}
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	classes := map[string]FirstBytes{"need_more": FirstBytesNeedMore, "record": FirstBytesRecord, "other": FirstBytesOther}
	for _, v := range doc.Vectors {
		b, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatal(err)
		}
		f := FramingByteStream
		if v.Framing == "records" {
			f = FramingRecords
		}
		c, need := ClassifyFirstBytes(b, f)
		if c != classes[v.Class] || need != v.Need {
			t.Errorf("%s (%s %s): %v need %d, want %s need %d", v.Note, v.Hex, v.Framing, c, need, v.Class, v.Need)
		}
		if need > MaxFirstBytes {
			t.Errorf("%s: need %d", v.Note, need)
		}
	}
	if len(doc.Vectors) < 40 {
		t.Fatalf("%d vectors", len(doc.Vectors))
	}
}

// The first frame types are the ones the session accepts as first
// records, and their leads are ff, 9a and ba.
func TestClassifyFirstBytesTable(t *testing.T) {
	if firstLeads != [3]byte{0xff, 0x9a, 0xba} {
		t.Fatalf("leads %x", firstLeads)
	}
	for _, ty := range wire.FirstFrameTypes {
		for _, size := range []uint64{30, 200, 20000} {
			rec := varint.AppendVarint(nil, size)
			rec = varint.AppendVarint(rec, ty)
			if c, _ := core.Classify(rec, MaxRecordSize); c == core.ClassReject {
				t.Fatalf("type %x size %d: core rejects", ty, size)
			}
			if c, _ := ClassifyFirstBytes(rec, FramingByteStream); c != FirstBytesRecord {
				t.Fatalf("type %x size %d: %v", ty, size, c)
			}
		}
	}
}

// Every prefix that the session's first-record parser does not reject is
// not judged Other, and every byte pair of 16 03 is.
func TestClassifyFirstBytesAgreesWithCore(t *testing.T) {
	for b0 := 0; b0 < 256; b0++ {
		for b1 := 0; b1 < 256; b1++ {
			for _, tail := range [][]byte{{0xff, 0x9a, 0xba}, {0x00, 0x40, 0xff}, {0x00, 0xff, 0xff}} {
				b := append([]byte{byte(b0), byte(b1)}, tail...)
				cc, _ := core.Classify(b, MaxRecordSize)
				c, _ := ClassifyFirstBytes(b, FramingByteStream)
				if cc != core.ClassReject && cc != core.ClassMore && c == FirstBytesOther {
					t.Fatalf("% x: core %v, classified Other", b, cc)
				}
			}
		}
	}
	if c, _ := ClassifyFirstBytes([]byte{0x16, 0x03}, FramingByteStream); c != FirstBytesOther {
		t.Fatal("16 03 not Other")
	}
}
