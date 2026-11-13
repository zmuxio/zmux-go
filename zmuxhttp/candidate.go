package zmuxhttp

import (
	"context"
	"net/http"

	"github.com/zmuxio/zmux-go"
)

// StreamCandidate is a bidirectional streaming request (StreamPipe) to
// url with client and the fields h. It passes intermediaries that forward
// both bodies at once: HTTP/2 or HTTP/3 to the server, or HTTP/1.1 hops
// that do not buffer request bodies.
func StreamCandidate(client *http.Client, url string, h http.Header) zmux.Candidate {
	t := NewTarget(url)
	return zmux.Candidate{Kind: zmux.CarrierStream, Target: t, Dial: func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		return httpPipe(ctx, client, url, t, h, zmux.DirectionBidirectional, cred)
	}}
}

// SplitCandidate is a split bootstrap (SplitPipe) to url with client
// and the fields h. As a further pipe of a session it joins as an uplink
// POST and a downlink GET that one Join pairs into one pipe. Its carriers must
// be confidential end to end or up to a trusted TLS-terminating
// intermediary.
func SplitCandidate(client *http.Client, url string, h http.Header) zmux.Candidate {
	return splitCandidate(client, url, h, HeaderPair)
}

// Candidate is SplitCandidate with o's pairing field.
func (o *SplitOptions) Candidate(client *http.Client, url string, h http.Header) (zmux.Candidate, error) {
	if err := o.Validate(); err != nil {
		return zmux.Candidate{}, err
	}
	return splitCandidate(client, url, h, o.pairField()), nil
}

func splitCandidate(client *http.Client, url string, h http.Header, pair string) zmux.Candidate {
	t := NewTarget(url)
	return zmux.Candidate{Kind: zmux.CarrierSplit, Target: t,
		Dial: func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
			return httpSplitPipe(ctx, client, url, t, h, cred, pair)
		},
		Join: func(ctx, _ context.Context, c *zmux.Conn, cred zmux.CarrierCredential, o zmux.PipeOptions) ([]*zmux.Pipe, error) {
			p, err := httpSplitJoin(ctx, c, client, url, t, h, cred, o, pair)
			if err != nil {
				return nil, err
			}
			return []*zmux.Pipe{p}, nil
		}}
}

// ExchangeCandidate is an exchange carrier (ExchangePipe) to url with
// client and cfg. It passes intermediaries that buffer whole bodies; it
// authenticates with cfg's PSK, and the session's credential is not
// presented on it.
func ExchangeCandidate(client *http.Client, url string, cfg *ExchangeConfig) zmux.Candidate {
	t := NewTarget(url)
	return zmux.Candidate{Kind: zmux.CarrierExchange, Target: t, Dial: func(ctx context.Context, _ zmux.CarrierCredential) (zmux.PipeIO, error) {
		return httpExchangePipe(ctx, client, url, t, cfg)
	}}
}
