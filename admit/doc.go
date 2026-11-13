// Package admit holds what a server uses to judge a carrier before any
// zmux handshake, and what a client presents for that judgment:
// ClassifyFirstBytes and FirstRecordIsLane tell zmux first records from
// other content in a carrier's first bytes, PrefixAuth is a credential of
// keyed, time-slotted tokens with its verifier (PrefixAuthVerifier, an
// admission decision), and ReplayFilter accepts each nonce or token once
// within a window. An Admission on a zmux.Listener and a transform's
// zmux.ListenerConfig.WrapPipe build on them.
package admit
