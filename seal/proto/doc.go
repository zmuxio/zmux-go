// Package proto is the sans-IO core of the Establish handshake of the zmux
// secure carrier: its wire encoding, state machine and payloads, transcript
// signatures and the key derivations of the secure-carrier specification. Join
// and Sealed records are in the root module's internal/join, shared with this
// package; importing it also registers Join Suite 2.
//
// It holds bytes and keys only: no IO, clock, callback or goroutine. The
// caller moves bytes, decides trust from what the handshake reveals and
// decides what to do after a failure. Every failure of a Handshake, a Sealer
// or an Opener is permanent.
//
// An initiator runs
//
//	hs, _ := proto.NewInitiator(cfg)
//	msg1, _ := hs.WriteMessage(nil, proto.Payload{})
//	wire, _ := proto.AppendHello(nil, proto.Stream, proto.Hello{Profile: cfg.Profile, Mode: cfg.Mode, Handle: cfg.Handle, Msg1: msg1})
//
// and a responder calls ParseHello, chooses its Config from the Hello and
// feeds Hello.Msg1 to ReadMessage. Both continue with WriteMessage and
// ReadMessage in turn until Finished, then call Finish.
package proto
