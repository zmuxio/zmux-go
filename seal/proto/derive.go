package proto

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/join"
)

const (
	labelExporter = "zmux-seal exporter"

	// BoundExporterLabel is the carrier exporter label of Bound; it is read
	// without a context, in BoundExporterLen bytes.
	BoundExporterLabel = "EXPORTER-zmux-seal-bound"
	// BoundExporterLen is the length of a carrier exporter and a Binding.
	BoundExporterLen = join.BindingLen

	labelToken = "zmux-seal token"
	// MinTokenSecret is the shortest secret of DeriveHandlePSK.
	MinTokenSecret = 16

	tokenHandleLen = 16
	tokenPSKLen    = 32
)

var expand = join.Expand

func exporterSecret(s suite, ck, h []byte) []byte {
	return expand(s.hash.New, ck, labelExporter+string(h), s.hash.Size())
}

// BoundExporter computes the Binding from the exporters of the uplink and
// downlink halves of a carrier, which are the same value on one connection.
// A half without an exporter is ErrInvalidConfig.
func BoundExporter(up, down []byte) ([]byte, error) { return join.CombineBinding(up, down) }

// DeriveHandlePSK derives a Handle and a pre-shared key from a high-entropy
// secret of at least MinTokenSecret bytes, within a domain. salt is an optional
// per-deployment value that takes batch attacks across deployments away. It is
// not for passwords: the Handle itself lets anyone test guesses offline.
func DeriveHandlePSK(secret, domain, salt []byte) (handle, psk []byte, err error) {
	if len(secret) < MinTokenSecret || len(domain) > 65535 {
		return nil, nil, fmt.Errorf("%w: token secret or domain", ErrInvalidConfig)
	}
	prk, err := hkdf.Extract(sha256.New, secret, append([]byte(labelToken), salt...))
	if err != nil {
		return nil, nil, err
	}
	info := func(label string) string {
		b := binary.BigEndian.AppendUint16([]byte(label), uint16(len(domain)))
		return string(append(b, domain...))
	}
	return expand(sha256.New, prk, info("handle"), tokenHandleLen), expand(sha256.New, prk, info("psk"), tokenPSKLen), nil
}
