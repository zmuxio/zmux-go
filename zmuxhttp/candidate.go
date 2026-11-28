package zmuxhttp

import (
	"context"

	"github.com/zmuxio/zmux-go"
)

// Candidate registers the bidirectional streaming HTTP carrier.
func (s Streaming) Candidate() zmux.Candidate {
	t := NewTarget(s.URL)
	return httpCandidate(zmux.CarrierStream, t, func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		return httpPipe(ctx, s.Client, s.URL, t, s.Header, zmux.DirectionBidirectional, either(s.CarrierCredential, cred))
	})
}

// Candidate registers a paired HTTP POST and GET as one attachment.
func (s Split) Candidate() zmux.Candidate {
	t := NewTarget(s.URL)
	pair, bad := s.pairField(), s.Validate()
	return httpCandidate(zmux.CarrierSplit, t, func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		if bad != nil {
			return zmux.PipeIO{}, bad
		}
		return httpSplitPipe(ctx, s.Client, s.URL, t, s.Header, either(s.CarrierCredential, cred), pair)
	})
}

// Candidate registers the exchange carrier for buffered HTTP requests.
func (e Exchange) Candidate() zmux.Candidate {
	t := NewTarget(e.URL)
	return httpCandidate(zmux.CarrierExchange, t, func(ctx context.Context, _ zmux.CarrierCredential) (zmux.PipeIO, error) {
		return e.pipe(ctx, t)
	})
}

func httpCandidate(kind zmux.CarrierKind, target *Target, open func(context.Context, zmux.CarrierCredential) (zmux.PipeIO, error)) zmux.Candidate {
	return zmux.Candidate{Kind: kind, CanStart: true, Attachments: []zmux.AttachmentSpec{{Directions: zmux.Both}},
		Open: func(ctx context.Context, req zmux.OpenRequest) ([]zmux.PipeIO, error) {
			p, err := open(ctx, req.Credential)
			if err != nil {
				target.Reset()
			}
			return []zmux.PipeIO{p}, err
		}}
}

func either(own, session zmux.CarrierCredential) zmux.CarrierCredential {
	if own != nil {
		return own
	}
	return session
}
