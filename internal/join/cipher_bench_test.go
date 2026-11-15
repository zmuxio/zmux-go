package join

import "testing"

// BenchmarkCipherRecord seals and opens one 1 KiB record: the record
// path of a Sealed carrier, which allocates nothing per record.
func BenchmarkCipherRecord(b *testing.B) {
	var enc, dec CipherState
	enc.Fn, dec.Fn = AESGCM{}, AESGCM{}
	k := make([]byte, 32)
	enc.Init(k)
	dec.Init(k)
	pt := make([]byte, 1024)
	ct := make([]byte, 0, 2048)
	out := make([]byte, 0, 2048)
	b.ReportAllocs()
	for b.Loop() {
		c, _ := enc.Encrypt(ct[:0], nil, pt)
		if _, err := dec.Decrypt(out[:0], nil, c); err != nil {
			b.Fatal(err)
		}
	}
}
