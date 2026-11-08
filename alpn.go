package zmux

import "strings"

// DefaultALPN is the default application protocol identifier, for ALPN
// and for carriers that negotiate it otherwise, such as a WebSocket
// subprotocol: QMux draft-02 with the application agreed out of band.
// Applications with an identity of their own use
// "qmux-02.<application>" or an identifier of their own.
const DefaultALPN = "qmux-02"

// ALPNAlias returns the draft-01 alias of a configured identifier,
// compared byte for byte: qmux-02 has the alias qmux-01,
// qmux-02.<application> has qmux-01.<application>, any other identifier
// that ends in -02 has the one that ends in -01, and any other identifier
// has none. A server accepts a configured identifier offered as it is or
// as its alias; a client may offer the aliases after its identifiers.
func ALPNAlias(id string) (string, bool) {
	if id == DefaultALPN {
		return "qmux-01", true
	}
	if app, ok := strings.CutPrefix(id, DefaultALPN+"."); ok {
		return "qmux-01." + app, true
	}
	if stem, ok := strings.CutSuffix(id, "-02"); ok {
		return stem + "-01", true
	}
	return "", false
}
