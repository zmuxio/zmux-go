// Package seal is the secure carrier of zmux: a carrier transform that runs the
// Establish handshake of a session's first pipe and takes the session's secret from it.
// Each carrier has a record mode. Sealed protects every zmux record with the handshake's
// keys, for carriers whose TLS ends at an intermediary that must neither read nor alter
// them. Bound leaves records to a carrier that protects them end to end and binds that
// carrier's exporter into the handshake. Trusted leaves records to the deployment, by
// explicit configuration of both ends.
//
// Client: Establish wraps a dialled carrier into a PipeIO for zmux.Start; give it a
// context with a time limit, since a server says nothing about a refused handshake
// (ErrNoReply). Server: set ListenerConfig.WrapPipe to a *ServerConfig, which
// zmux.NewListener validates. Join is not this package's concern:
// the session handles it on every further carrier, and the transform passes a carrier
// whose first byte is 02 on untouched.
//
// Who the peer is, is the host's decision. An Anchor (ClientConfig.Anchor,
// ServerConfig.ClientAnchor) judges the peer once per handshake from a VerifyInput: the
// peer's static key, its one opaque credential, the PSK identity, the host's
// LocalContext and the application data received so far. Its Verdict names the peer and
// may ask for a transcript signature by a key it names; the handshake checks that
// signature, and a post-quantum one makes the peer PQ-authenticated (Peer.PQAuthenticated,
// required with RequirePQAuth). PinAnchor trusts one static key, optionally with a
// signing key. An error matching ErrVerifyRejected rejects the peer and one
// matching ErrVerifyUnavailable is temporary; both end the handshake. A Gate
// finds the pre-shared key of a Handle, supplies the node's credential and
// accepts the client (Gate.Commit).
//
// Each end can carry opaque application data in its messages through an AppDataSource:
// the client in message 1 (Profiles with a pre-shared key) and message 3, the server in
// message 2. The peer reads it on VerifyInput.PeerAppData and Peer.AppData, each piece with
// its message and whether it is forward secret and authenticated.
//
// No failure falls back to another mode or profile on the same carrier; choosing
// differently takes a new carrier and is the host's.
//
// A process restored from a snapshot MUST reseed its random source before any Join: a
// repeated random output can repeat a Join's record keys and nonces.
package seal

import (
	"runtime"

	"golang.org/x/sys/cpu"

	"github.com/zmuxio/zmux-go/internal/join"
)

// hasAESHardware reports AES-GCM in hardware on this machine.
func hasAESHardware() bool {
	switch runtime.GOARCH {
	case "amd64":
		return cpu.X86.HasAES && cpu.X86.HasPCLMULQDQ
	case "arm64":
		return runtime.GOOS == "darwin" || cpu.ARM64.HasAES && cpu.ARM64.HasPMULL
	case "s390x":
		return cpu.S390X.HasAES && cpu.S390X.HasAESGCM
	case "ppc64", "ppc64le":
		return true
	}
	return false
}

func init() { join.PreferChaCha = !hasAESHardware() }
