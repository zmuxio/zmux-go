package zmuxhttp

import (
	"context"

	"github.com/zmuxio/zmux-go"
)

// Candidate is the streaming carrier as a candidate of a carrier choice: a
// bidirectional streaming request (Streaming.Pipe). It presents the
// CarrierCredential of s, else the session's (Config.CarrierCredential), as
// the split candidate does. It passes
// intermediaries that forward both bodies at once: HTTP/2 or HTTP/3 to the
// server, or HTTP/1.1 hops that do not buffer request bodies.
func (s Streaming) Candidate() zmux.Candidate {
	t := NewTarget(s.URL)
	return zmux.Candidate{Kind: zmux.CarrierStream, Target: t, Dial: func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		return httpPipe(ctx, s.Client, s.URL, t, s.Header, zmux.DirectionBidirectional, either(s.CarrierCredential, cred))
	}}
}

// Candidate is the split carrier as a candidate of a carrier choice
// (Split.Pipe). As a further pipe of a session it joins as an uplink POST
// and a downlink GET that one Join pairs into one pipe. Its carriers must
// be confidential end to end or up to a trusted TLS-terminating
// intermediary. An invalid s (Validate) fails every dial and join of
// the candidate with its error.
func (s Split) Candidate() zmux.Candidate {
	t := NewTarget(s.URL)
	pair := s.pairField()
	bad := s.Validate()
	return zmux.Candidate{Kind: zmux.CarrierSplit, Target: t,
		Dial: func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
			if bad != nil {
				return zmux.PipeIO{}, bad
			}
			return httpSplitPipe(ctx, s.Client, s.URL, t, s.Header, either(s.CarrierCredential, cred), pair)
		},
		Join: func(ctx, _ context.Context, c *zmux.Conn, cred zmux.CarrierCredential, o zmux.PipeOptions) ([]*zmux.Pipe, error) {
			if bad != nil {
				return nil, bad
			}
			p, err := httpSplitJoin(ctx, c, s.Client, s.URL, t, s.Header, either(s.CarrierCredential, cred), o, pair)
			if err != nil {
				return nil, err
			}
			return []*zmux.Pipe{p}, nil
		}}
}

// Candidate is the exchange carrier as a candidate of a carrier choice
// (Exchange.Pipe). It passes intermediaries that buffer whole bodies; it
// authenticates with Config's PSK, and the session's credential is not
// presented on it.
func (e Exchange) Candidate() zmux.Candidate {
	t := NewTarget(e.URL)
	return zmux.Candidate{Kind: zmux.CarrierExchange, Target: t, Dial: func(ctx context.Context, _ zmux.CarrierCredential) (zmux.PipeIO, error) {
		return e.pipe(ctx, t)
	}}
}

// either returns own when set, else the session's credential.
func either(own, session zmux.CarrierCredential) zmux.CarrierCredential {
	if own != nil {
		return own
	}
	return session
}
