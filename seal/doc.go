// Package seal is an end-to-end protected carrier for zmux: a carrier
// transform that runs a Noise handshake on each carrier and protects every
// zmux record with AES-256-GCM, for carriers whose TLS ends at an
// intermediary (a CDN or reverse proxy) that must neither read nor alter
// them.
//
// EXPERIMENTAL: the wire format and the API may change in any release,
// without compatibility with earlier versions.
//
// Two handshakes share one entrance, told apart by the server without any
// marker byte:
//
//   - regular, Noise_IKpsk1_25519_AESGCM_SHA256: the client pins the
//     server's static keys (ClientConfig.Peers) and authenticates with its
//     own static key and an optional PSK (ServerConfig.Authorize);
//   - bootstrap, Noise_NXpsk0_25519_AESGCM_SHA256: the client knows only a
//     PSK, names itself with a tag that changes every 10 minutes
//     (TagTable), and learns the server's static key (ClientConfig.OnPeer).
//
// Both take one round trip, and msg1 carries the client's first flight
// (use zmux.EarlyData{ZmuxPeer, Replayable, HoldFirstFlight} and
// Conn.ReleaseFirstFlight to put the first streams into it). A first
// flight whose stamp is outside the server's window, or that the server
// saw before, is refused without failing the handshake: the client sends
// it again under the transport keys. The server's msg2 carries its time,
// from which clients learn the offset of their clock (ClockSkew).
//
// Client: Client wraps a dialled carrier, WrapCandidate a zmux.Candidate,
// Dial dials TCP. Server: set ListenerConfig.Admission to
// Server.Admission(SealedOnly or Auto) and ListenerConfig.WrapPipe to
// Server.WrapPipe; zmux.Conn.Admitted then returns a *Peer. Both ends
// export keying material from the handshake (zmux.KeyingMaterialExporter),
// so that zmux.PipeAuth.Exporter and carrier-bound joins bind to it; each
// carrier, joins included, runs its own handshake. Split carriers are not
// supported.
package seal
