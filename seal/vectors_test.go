package seal

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go"
)

var update = flag.Bool("update", false, "rewrite testdata/seal/vectors.json")

const vectorsPath = "../testdata/seal/vectors.json"

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func xkey(t testing.TB, s string) *ecdh.PrivateKey {
	k, err := ecdh.X25519().NewPrivateKey(unhex(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Noise test vectors (cacophony format) for the two patterns, with an
// empty prologue and plain Noise messages: msg 0 and 1 are the handshake,
// msg 2 (initiator) and 3 (responder) transport messages.
var noiseVectors = []struct {
	name, initStatic, respStatic, initEph, respEph, psk string
	payloads, cts                                       [4]string
}{
	{name: nameIK,
		initStatic: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		respStatic: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		initEph:    "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		respEph:    "4142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		psk:        "2176657279736563726574766572797365637265747665727973656372657421",
		payloads:   [4]string{"", "", "79656c6c6f777375626d6172696e65", "7375626d6172696e6579656c6c6f77"},
		cts: [4]string{
			"358072d6365880d1aeea329adf9121383851ed21a28e3b75e965d0d2cd166254d090a76917ed86b1ca3f8af8ac5c0803d5b3b290ab95fa415d8bf2f9200a59fc34c54450e0a71f213dbb91f5a5843c28a1b1e94ea7a8c729788b074f894477f7",
			"64b101b1d0be5a8704bd078f9895001fc03e8e9f9522f188dd128d9846d4846635c80f45db0193be6c76fa63c43b2230",
			"245e2f9694b825a856dc97709fcc450870d23dd07637b57d21268ad60016e4",
			"977eb8234bef8ece7a14c771fa5019aae42c0f4655d4e1ffbfdb4a96def193"}},
	{name: nameNX,
		respStatic: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		initEph:    "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		respEph:    "4142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		psk:        "2176657279736563726574766572797365637265747665727973656372657421",
		payloads:   [4]string{"", "", "79656c6c6f777375626d6172696e65", "7375626d6172696e6579656c6c6f77"},
		cts: [4]string{
			"358072d6365880d1aeea329adf9121383851ed21a28e3b75e965d0d2cd166254600c4a8eb4f41bafda4465f2e9e34bae",
			"64b101b1d0be5a8704bd078f9895001fc03e8e9f9522f188dd128d9846d484661aa7a9691509a04c46a6ce79f30fdbc377a2158ca3967df0ea4b1bb18532ca8737927e184c0605e9641bc3974b037282ceae988c87be4c68494c1d4f2b705cb2",
			"309e81a1ec83fd198e19b766e75f5e3a6ee55cd7b0119955211dcfcfdd0dae",
			"92ac33cb48df9677f9a6528346e17cd89855368535e8ee3c8252291d7820a4"}},
}

func TestNoiseVectors(t *testing.T) {
	plain := hsParams{}
	for _, v := range noiseVectors {
		nx := v.name == nameNX
		ini := &handshake{p: plain, nx: nx, e: xkey(t, v.initEph)}
		res := &handshake{p: plain, nx: nx, s: xkey(t, v.respStatic)}
		copy(ini.psk[:], unhex(t, v.psk))
		res.psk = ini.psk
		if !nx {
			ini.s = xkey(t, v.initStatic)
			copy(ini.rs[:], res.s.PublicKey().Bytes())
		}
		m0, err := ini.writeMsg1(nil, nil, unhex(t, v.payloads[0]))
		if err != nil || !bytes.Equal(m0, unhex(t, v.cts[0])) {
			t.Fatalf("%s msg 0: %x %v", v.name, m0, err)
		}
		if nx {
			_, _, err = res.readNX(m0)
		} else if err = res.readIKStatic(m0); err == nil {
			_, _, err = res.readIKRest(m0)
		}
		if err != nil {
			t.Fatalf("%s read msg 0: %v", v.name, err)
		}
		res.e = xkey(t, v.respEph)
		m1, err := res.writeMsg2(nil, unhex(t, v.payloads[1]))
		if err != nil || !bytes.Equal(m1, unhex(t, v.cts[1])) {
			t.Fatalf("%s msg 1: %x %v", v.name, m1, err)
		}
		if _, _, err := ini.readMsg2(m1); err != nil {
			t.Fatalf("%s read msg 1: %v", v.name, err)
		}
		i1, _ := ini.ss.split()
		_, r2 := res.ss.split()
		m2, _ := i1.seal(nil, nil, unhex(t, v.payloads[2]))
		m3, _ := r2.seal(nil, nil, unhex(t, v.payloads[3]))
		if !bytes.Equal(m2, unhex(t, v.cts[2])) || !bytes.Equal(m3, unhex(t, v.cts[3])) {
			t.Fatalf("%s transport: %x %x", v.name, m2, m3)
		}
	}
}

// vectors is testdata/seal/vectors.json: this package's own wire vectors.
type vectors struct {
	Note       string            `json:"note"`
	Handshakes []hsVector        `json:"handshakes"`
	Frames     []frameVector     `json:"frames"`
	Keys       map[string]string `json:"keys"`
}

type hsVector struct {
	Mode            string `json:"mode"` // "IKpsk1" or "NXpsk0"
	ClientStatic    string `json:"client_static,omitempty"`
	ServerStatic    string `json:"server_static"`
	ClientEphemeral string `json:"client_ephemeral"`
	ServerEphemeral string `json:"server_ephemeral"`
	PSK             string `json:"psk"` // the host PSK, before SHA-256
	Tag             string `json:"tag,omitempty"`
	Payload1        string `json:"payload1"`
	Payload2        string `json:"payload2"`
	Msg1            string `json:"msg1"`
	Msg2            string `json:"msg2"`
	ReplayKey       string `json:"replay_key"`
	Exporter        string `json:"exporter"` // label "test", context "ctx", 32 bytes
	ClientFrame0    string `json:"client_frame0"`
}

type frameVector struct {
	Key     string `json:"key"`
	Seq     uint64 `json:"seq"`
	Record  string `json:"record"`
	Stream  string `json:"stream"`  // Len ‖ AEAD
	Message string `json:"message"` // AEAD
}

func hx(b []byte) string { return hex.EncodeToString(b) }

func genVectors(t *testing.T) vectors {
	cs := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	ss := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	ce := "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
	se := "4142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60"
	psk := []byte("0123456789abcdef0123456789abcdef")
	// payload1: stamp 1767225600000 (2026-01-01) and one record; payload2:
	// server time, flags 1, no record.
	p1 := binary.BigEndian.AppendUint64(nil, 1767225600000)
	p1 = appendRecordsOne(p1, unhex(t, "ff0102030405060708"))
	p2 := append(binary.BigEndian.AppendUint64(nil, 1767225601000), flagEarlyAccepted)
	v := vectors{Note: "seal wire vectors (experimental format); hex; see seal/vectors_test.go"}
	for _, nx := range []bool{false, true} {
		hv := hsVector{Mode: "IKpsk1", ServerStatic: ss, ClientEphemeral: ce, ServerEphemeral: se, PSK: hx(psk),
			Payload1: hx(p1), Payload2: hx(p2)}
		ini := &handshake{p: defaultParams, nx: nx, e: xkey(t, ce), psk: compressPSK(psk)}
		res := &handshake{p: defaultParams, nx: nx, s: xkey(t, ss), psk: ini.psk}
		var tag [nxTagLen]byte
		if nx {
			hv.Mode = "NXpsk0"
			tag = tagFor(&ini.psk, 1767225600000)
			hv.Tag = hx(tag[:])
		} else {
			hv.ClientStatic = cs
			ini.s = xkey(t, cs)
			copy(ini.rs[:], res.s.PublicKey().Bytes())
		}
		m1, err := ini.writeMsg1(nil, &tag, p1)
		if err != nil {
			t.Fatal(err)
		}
		var got []byte
		if nx {
			got, _, err = res.readNX(m1)
		} else if err = res.readIKStatic(m1); err == nil {
			got, _, err = res.readIKRest(m1)
		}
		if err != nil || !bytes.Equal(got, p1) {
			t.Fatalf("msg1 round trip: %v", err)
		}
		res.e = xkey(t, se)
		m2, err := res.writeMsg2(nil, p2)
		if err != nil {
			t.Fatal(err)
		}
		if got, _, err = ini.readMsg2(m2); err != nil || !bytes.Equal(got, p2) {
			t.Fatalf("msg2 round trip: %v", err)
		}
		sec := ini.exporterSecret()
		if sec != res.exporterSecret() {
			t.Fatal("exporter secrets differ")
		}
		ex, _ := export(&sec, "test", []byte("ctx"), 32)
		c1, _ := ini.ss.split()
		fs := newFrameState(c1)
		f0, _ := fs.seal(nil, []byte{0xff, 0x00}, true)
		hv.Msg1, hv.Msg2, hv.ReplayKey, hv.Exporter, hv.ClientFrame0 = hx(m1), hx(m2), hx(res.h1[:]), hx(ex), hx(f0)
		v.Handshakes = append(v.Handshakes, hv)
	}
	key := unhex(t, ss)
	rec := []byte("record")
	for _, seq := range []uint64{0, 1, rekeyInterval - 1, rekeyInterval, 2 * rekeyInterval} {
		var cs cipherState
		cs.init(key)
		f := newFrameState(cs)
		for f.next <= seq { // the rekeys a carrier ran before seq
			f.cs.rekey()
			f.next += rekeyInterval
		}
		f.cs.n = seq
		g := f
		st, _ := f.seal(nil, rec, true)
		msg, _ := g.seal(nil, rec, false)
		v.Frames = append(v.Frames, frameVector{Key: hx(key), Seq: seq, Record: hx(rec), Stream: hx(st), Message: hx(msg)})
	}
	sk := KeyFromSecret([]byte("vkey-0123456789ab"), "node")
	edSeed := unhex(t, ce)
	ed := ed25519.NewKeyFromSeed(edSeed)
	xp, err := X25519FromEd25519Public(ed.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	xs := X25519FromEd25519Private(ed)
	if xs.Public() != xp {
		t.Fatal("Ed25519 conversions disagree")
	}
	v.Keys = map[string]string{
		"key_from_secret(secret=vkey-0123456789ab,label=node)": hx(sk[:]),
		"ed25519_seed":          hx(edSeed),
		"x25519_from_ed25519":   hx(xs[:]),
		"x25519_public_from_ed": hx(xp[:]),
	}
	return v
}

func appendRecordsOne(b, r []byte) []byte {
	out, _ := appendRecords(b, [][]byte{r}, maxPayload)
	return out
}

func TestVectors(t *testing.T) {
	v := genVectors(t)
	got, _ := json.MarshalIndent(v, "", "  ")
	got = append(got, '\n')
	if *update {
		if err := os.WriteFile(vectorsPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("vectors differ from testdata/seal/vectors.json (go test -run TestVectors -update)")
	}
}

// Ephemeral keys never look like a zmux first record or TLS.
func TestEphemeralResample(t *testing.T) {
	for _, b := range [][]byte{{0x16, 0x03, 0, 0, 0}, {0x09, 0xff, 0, 0, 0}, {0xff, 0, 0, 0, 0}, {0x40, 0x10, 0x9a, 0, 0}} {
		if firstBytesOK(b) {
			t.Fatalf("% x accepted", b)
		}
	}
	if !firstBytesOK([]byte{0x01, 0x02, 0x03, 0x04, 0x05}) {
		t.Fatal("01 02 refused")
	}
	for range 2000 {
		k, err := genEphemeral(nil)
		if err != nil {
			t.Fatal(err)
		}
		pub := k.PublicKey().Bytes()
		for _, f := range []zmux.Framing{zmux.FramingByteStream, zmux.FramingRecords} {
			if c, _ := zmux.ClassifyFirstBytes(pub, f); c != zmux.FirstBytesOther {
				t.Fatalf("% x is %v", pub[:5], c)
			}
		}
	}
}

// Tags: the table finds a client within ±2 buckets of its clock.
func TestTagBuckets(t *testing.T) {
	psk := []byte("0123456789abcdef")
	k := compressPSK(psk)
	clk := newFakeClock()
	tt := NewTagTable(clk)
	if err := tt.Set("a", psk, 7); err != nil {
		t.Fatal(err)
	}
	now := clk.Now().UnixMilli()
	for d := int64(-3); d <= 3; d++ {
		_, ok := tt.lookup(tagFor(&k, now+d*bucketMs))
		if ok != (d >= -2 && d <= 2) {
			t.Fatalf("bucket %+d: %v", d, ok)
		}
	}
	clk.Advance(3 * bucketMs * time.Millisecond) // roll by three buckets
	if _, ok := tt.lookup(tagFor(&k, now)); ok {
		t.Fatal("old bucket still found")
	}
	if _, ok := tt.lookup(tagFor(&k, now+5*bucketMs)); !ok {
		t.Fatal("new bucket missing")
	}
	tt.Delete("a")
	if _, ok := tt.lookup(tagFor(&k, clk.Now().UnixMilli())); ok || tt.Len() != 0 {
		t.Fatal("deleted client found")
	}
}

// The record path allocates nothing per frame.
func TestFrameAllocs(t *testing.T) {
	var cs cipherState
	cs.init(make([]byte, 32))
	s, r := newFrameState(cs), newFrameState(cs)
	rec := make([]byte, 1400)
	buf := make([]byte, 0, 2048)
	n := testing.AllocsPerRun(100, func() {
		f, err := s.seal(buf[:0], rec, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.open(f, len(rec)); err != nil {
			t.Fatal(err)
		}
	})
	if n != 0 {
		t.Fatalf("%v allocations per frame", n)
	}
}

func BenchmarkFrame(b *testing.B) {
	for _, size := range []int{1400, 16 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			var cs cipherState
			cs.init(make([]byte, 32))
			s, r := newFrameState(cs), newFrameState(cs)
			rec := make([]byte, size)
			buf := make([]byte, 0, size+32)
			b.SetBytes(int64(size))
			for b.Loop() {
				f, _ := s.seal(buf[:0], rec, false)
				if _, err := r.open(f, size); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHandshakeIK(b *testing.B) {
	sk, _ := GenerateKey()
	ck, _ := GenerateKey()
	psk := compressPSK([]byte("0123456789abcdef"))
	srv := sk.ecdh()
	for b.Loop() {
		e, _ := genEphemeral(nil)
		ini := &handshake{p: defaultParams, e: e, s: ck.ecdh(), rs: sk.Public(), psk: psk}
		m1, _ := ini.writeMsg1(nil, nil, make([]byte, 64))
		res := &handshake{p: defaultParams, s: srv, psk: psk}
		if err := res.readIKStatic(m1); err != nil {
			b.Fatal(err)
		}
		if _, _, err := res.readIKRest(m1); err != nil {
			b.Fatal(err)
		}
		res.e, _ = genEphemeral(nil)
		m2, _ := res.writeMsg2(nil, make([]byte, 9))
		if _, _, err := ini.readMsg2(m2); err != nil {
			b.Fatal(err)
		}
	}
}

func newFakeClock() *timeshim.Fake { return timeshim.NewFake(time.UnixMilli(1767225600000)) }
