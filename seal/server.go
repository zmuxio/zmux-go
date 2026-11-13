package seal

import (
	"context"
	"crypto/ecdh"
	"errors"
	"fmt"
	"net/netip"
	"slices"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/seal/proto"
	"github.com/zmuxio/zmux-go/shaping"
)

// Gate is the server's host: it decides who may establish.
type Gate interface {
	// Resolve returns the pre-shared key of a Handle. It runs before any
	// authentication and has no side effect; an unknown Handle is an error (the
	// local reason unknown_handle). A host negatively caches what it looks up.
	Resolve(ctx context.Context, handle []byte, src netip.AddrPort) ([]byte, error)
	// Credential returns the node's credential for message 2, nil: none.
	Credential(ctx context.Context, r CredentialRequest) (*Credential, error)
	// Commit accepts a client after message 3 passed. It is where any state that
	// takes effect only with a successful handshake is stored. Its value is
	// Peer.Value; an error closes the carrier.
	Commit(ctx context.Context, a Admission) (any, error)
}

// CredentialRequest is what Gate.Credential knows of a handshake: the gate
// chooses the credential the client can accept.
type CredentialRequest struct {
	Handle       []byte // nil on a profile without a pre-shared key
	Profile      proto.Profile
	Source       netip.AddrPort // may be the zero value
	LocalContext any
}

// Admission is what Gate.Commit receives of a client that passed message 3.
type Admission struct {
	Handle []byte         // nil on a profile without a pre-shared key
	Source netip.AddrPort // may be the zero value
	// Peer is the client: its static key, credential, the ClientAnchor's
	// verdict (zero without a ClientAnchor), and the Profile and Mode of the
	// handshake.
	Peer *Peer
}

// ServerConfig is the server's side of Establish. A *ServerConfig is a
// zmux.PipeWrapper for ListenerConfig.WrapPipe, and zmux.NewListener calls its
// Validate. Refusals go to ListenerConfig.OnReject, whose default stalls them;
// nothing is ever sent to report one.
type ServerConfig struct {
	// Gate is the host's decision on clients; required.
	Gate Gate
	// Key is the node's static key; required.
	Key *ecdh.PrivateKey
	// ClientAnchor, when set, judges the client after message 3 (zmux.RoleClient).
	// nil: clients are not authenticated beyond the pre-shared key.
	ClientAnchor Anchor
	// RequirePQAuth fails a client that is not authenticated by a post-quantum
	// transcript signature; it needs ClientAnchor.
	RequirePQAuth bool
	// Signer, when set, signs the transcript in message 2, for clients whose
	// Anchor asks for it. In Profiles 4 and 5 anyone can ask for a signature:
	// limit their rate in the Signer.
	Signer Signer
	// LocalContext is passed to Gate.Credential, AppData and the ClientAnchor.
	LocalContext any
	// AppData supplies the application data of message 2; its request holds
	// what is known of the client then (the Profile, the PSK identity and the
	// data of message 1; the client's static key and credential arrive in
	// message 3). nil: none.
	//
	// The client is not authenticated yet. In Profiles 4 and 5 the data goes
	// to anyone who connects, so it may hold only public content. Every
	// handshake that reaches message 2 calls it, unauthenticated ones
	// included: it must be light and without side effects, and anything that
	// queries other systems waits until the handshake completes (Gate.Commit).
	AppData AppDataSource
	// MaxAppData bounds the application data of a received message; zero is
	// proto.DefaultMaxAppData, negative no bound beyond the message.
	MaxAppData int
	// AcceptedProfiles are the Profiles accepted. nil or empty: the default
	// set, the Profiles with a pre-shared key (1 and 3). Listing a Profile
	// without one (4, 5), which sends the node's credential to anonymous
	// clients, is the only way to accept it.
	AcceptedProfiles []proto.Profile
	// AllowRecordMode is the check of a Hello's mode, with the Establish's Peer as its
	// peer. nil: only Sealed. Joins have their own check, Config.AllowRecordMode.
	AllowRecordMode zmux.AllowRecordMode
	// Shaper is asked for the plan of each handshake message; nil: none. It is
	// called from concurrent handshakes.
	Shaper shaping.HandshakeShaper
	// ShapingLimits limit those plans.
	ShapingLimits shaping.Limits
}

// WrapPipe runs the server's side of Establish on a new carrier. A carrier whose first
// byte is 02 is a Join and passes on untouched; any other first byte is refused.
func (c *ServerConfig) WrapPipe(ctx context.Context, p zmux.PipeIO, hint zmux.PipeHint) (zmux.PipeIO, error) {
	accepted, err := c.accepted()
	if err != nil {
		return zmux.PipeIO{}, err
	}
	f := framingOf(p)
	u := &unitReader{p: p, f: f}
	for {
		if f == proto.Message || len(u.buf) == 0 {
			if err := u.fill(); err != nil {
				return zmux.PipeIO{}, err
			}
		}
		switch proto.Dispatch(u.buf, f) {
		case join.ClassIncomplete:
			continue
		case join.ClassJoin:
			return zmux.PrefixedPipe(p, u.rest()), nil
		case join.ClassEstablish:
		default:
			fr := zmux.FramingByteStream
			if f == proto.Message {
				fr = zmux.FramingRecords
			}
			lane, need := admit.FirstRecordIsLane(u.buf, fr)
			if need > 0 {
				if err := u.fill(); err != nil {
					return zmux.PipeIO{}, err
				}
				continue
			}
			if lane {
				return zmux.PrefixedPipe(p, u.rest()), nil
			}
			return zmux.PipeIO{}, &zmux.RejectError{Reason: "unknown_profile"}
		}
		break
	}
	var h proto.Hello
	for {
		var err error
		h, _, err = proto.ParseHelloExact(u.buf, f)
		if err == nil {
			break
		}
		if !errors.Is(err, proto.ErrIncomplete) {
			return zmux.PipeIO{}, &zmux.RejectError{Reason: reasonOf(err)}
		}
		if err := u.fill(); err != nil {
			return zmux.PipeIO{}, err
		}
	}
	mode := zmux.RecordMode(h.Mode)
	reject := func(err error) (zmux.PipeIO, error) {
		if ctx.Err() != nil {
			return zmux.PipeIO{}, ctx.Err()
		}
		return zmux.PipeIO{}, &zmux.RejectError{Profile: uint64(h.Profile), RecordMode: mode, Reason: reasonOf(err), Err: err}
	}
	if !slices.Contains(accepted, h.Profile) {
		return reject(errProfile)
	}
	withPSK := h.Profile.HasPSK()
	if mode != zmux.RecordSealed && c.AllowRecordMode == nil {
		return reject(errMode)
	}
	src := hint.Source
	var psk []byte
	if withPSK {
		var err error
		if psk, err = c.Gate.Resolve(ctx, h.Handle, src); err != nil {
			return reject(fmt.Errorf("%w: %w", errHandle, err))
		}
	}
	var binding []byte
	if mode == zmux.RecordBound {
		var err error
		// A Hello in Bound is never carrier early data; a carrier without the Bound
		// contract has no Binding to check it with, which ends as auth.
		if es, ok := p.W.(zmux.EarlyServerCarrier); ok && es.UsedEarlyData() {
			return reject(errEarly)
		}
		if binding, err = readBinding(ctx, p, false); err != nil {
			return reject(fmt.Errorf("%w: %w", errBinding, err))
		}
	}
	hs, err := proto.NewResponder(proto.Config{Profile: h.Profile, Mode: h.Mode, Handle: h.Handle, PSK: psk, Binding: binding, Static: c.Key,
		MaxAppData: c.MaxAppData})
	if err != nil {
		return reject(err)
	}
	first, err := hs.ReadMessage(h.Msg1)
	if err != nil {
		return reject(err)
	}
	early := appData(1, first.AppData)
	cred, err := c.Gate.Credential(ctx, CredentialRequest{Handle: h.Handle, Profile: h.Profile, Source: src, LocalContext: c.LocalContext})
	if err != nil {
		return reject(fmt.Errorf("%w: %w", errGate, err))
	}
	second := proto.Payload{Credential: cred, Proof: proofPlaceholder(c.Signer)}
	known := &VerifyInput{Role: zmux.RoleClient, Profile: h.Profile, PSKIdentity: h.Handle, PeerAppData: early, LocalContext: c.LocalContext}
	if second.AppData, err = askAppData(ctx, c.AppData, AppDataRequest{Message: 2, Peer: known}); err != nil {
		return reject(fmt.Errorf("%w: %w", errGate, err))
	}
	var padded uint64
	if err := shape(ctx, c.Shaper, &c.ShapingLimits, p, msgReply, hs, proto.FrameHeaderLen(f), &second, &padded); err != nil {
		return zmux.PipeIO{}, err
	}
	msg, err := writeProof(ctx, hs, second, c.Signer)
	if err != nil {
		return reject(err) // the key exchange with the Hello's keys failed (kex)
	}
	if msg, err = proto.AppendFrame(nil, f, msg); err != nil {
		return zmux.PipeIO{}, err
	}
	if err := writeUnit(p, msg); err != nil {
		return zmux.PipeIO{}, err
	}
	u.buf = nil
	finish, err := u.frame()
	if err != nil {
		if protocolError(err) {
			return reject(err)
		}
		return zmux.PipeIO{}, err // the carrier failed or ended
	}
	third, err := hs.ReadMessage(finish)
	if err != nil {
		return reject(err)
	}
	peer := &Peer{Static: append([]byte(nil), hs.PeerStatic()...), Credential: third.Credential,
		AppData: append(early, appData(3, third.AppData)...), Profile: h.Profile, RecordMode: mode,
		PQConfidential: h.Profile.PQConfidential()}
	if c.ClientAnchor != nil {
		in := VerifyInput{Role: zmux.RoleClient, Profile: h.Profile, PeerStatic: peer.Static, PSKIdentity: h.Handle,
			Credential: third.Credential, PeerAppData: slices.Clone(peer.AppData), LocalContext: c.LocalContext}
		v, pq, err := judge(ctx, c.ClientAnchor, in, third.Proof, c.RequirePQAuth)
		if e := ctx.Err(); e != nil {
			// Cancelled during or right after Verify: not a refusal.
			closeCarrier(p)
			return zmux.PipeIO{}, e
		}
		if errors.Is(err, ErrVerifyUnavailable) {
			return zmux.PipeIO{}, err // closes the carrier; not a refusal
		}
		if err != nil {
			return reject(err)
		}
		peer.Verdict, peer.PQAuthenticated = v, pq
		authenticated(peer.AppData)
	}
	if c.AllowRecordMode != nil && !c.AllowRecordMode(carrierInfo(p), mode, peer) {
		return reject(errMode)
	}
	if peer.Value, err = c.Gate.Commit(ctx, Admission{Handle: h.Handle, Source: src, Peer: peer}); err != nil {
		return reject(fmt.Errorf("%w: %w", errCommit, err))
	}
	res, err := hs.Finish()
	if err != nil {
		return reject(err)
	}
	q := wrapResult(p, u.rest(), res, peer)
	q.HandshakePadding += padded
	return q, nil
}

// Reasons of a refusal that has no error class in the protocol.
var (
	errProfile = errors.New("profile_disabled")
	errMode    = errors.New("record_mode")
	errEarly   = errors.New("early_data")
	errBinding = errors.New("binding_unavailable")
	errHandle  = errors.New("unknown_handle")
	errGate    = errors.New("gate_error")
	errCommit  = errors.New("commit")
)

func supported(p proto.Profile) bool { return p.Supported() }

// Validate reports a configuration that cannot serve (ErrInvalidConfig): a
// missing Key or Gate, or an unknown or reserved Profile among
// AcceptedProfiles. zmux.NewListener calls it for a ServerConfig in
// ListenerConfig.WrapPipe.
func (c *ServerConfig) Validate() error {
	_, err := c.accepted()
	return err
}

// accepted is the accepted set, or the configuration error.
func (c *ServerConfig) accepted() ([]proto.Profile, error) {
	switch {
	case c.Key == nil:
		return nil, fmt.Errorf("%w: ServerConfig without a Key", proto.ErrInvalidConfig)
	case c.Gate == nil:
		return nil, fmt.Errorf("%w: ServerConfig without a Gate", proto.ErrInvalidConfig)
	case c.RequirePQAuth && c.ClientAnchor == nil:
		return nil, fmt.Errorf("%w: RequirePQAuth without a ClientAnchor", proto.ErrInvalidConfig)
	}
	if err := checkPQAuth(c.RequirePQAuth, c.ClientAnchor); err != nil {
		return nil, err
	}
	if len(c.AcceptedProfiles) == 0 {
		return serverProfiles, nil
	}
	for _, p := range c.AcceptedProfiles {
		if !supported(p) {
			return nil, fmt.Errorf("%w: ServerConfig.AcceptedProfiles holds %d, not a supported Establish Profile", proto.ErrInvalidConfig, p)
		}
	}
	return c.AcceptedProfiles, nil
}

func carrierInfo(p zmux.PipeIO) zmux.RecordPolicyInput {
	_, exp := zmux.Exporter(p)
	return zmux.RecordPolicyInput{HasExporter: exp, Secure: zmux.IsCarrierSecure(p), Label: p.Label, IsFirstPipe: true}
}

// protocolError reports whether err is the peer's protocol violation, not
// a failure or the end of the carrier: those are rejected (OnReject), the
// others close the carrier.
func protocolError(err error) bool {
	return errors.Is(err, proto.ErrTooLarge) || errors.Is(err, proto.ErrMalformed)
}

// reasonOf is the local reject reason of a failure: the error class of
// the protocol, or a finer local reason.
func reasonOf(err error) string {
	for _, c := range []struct {
		err  error
		name string
	}{
		{proto.ErrMalformed, "malformed"}, {proto.ErrTrailing, "trailing_bytes"},
		{proto.ErrUnknownProfile, "unknown_profile"}, {proto.ErrBadFlags, "bad_flags"},
		{proto.ErrBadHandle, "bad_handle"}, {proto.ErrAuth, "auth"}, {proto.ErrKex, "kex"},
		{proto.ErrPeerIsSelf, "peer_is_self"}, {proto.ErrTooLarge, "too_large"},
		{proto.ErrTLVTruncated, "tlv_truncated"}, {proto.ErrTLVNonMinimal, "tlv_non_minimal"},
		{proto.ErrTLVOrder, "tlv_order"}, {proto.ErrTLVUnknownCritical, "tlv_unknown_critical"},
		{proto.ErrTLVCredentials, "tlv_credentials"}, {proto.ErrInvalidConfig, "invalid_config"},
		{ErrProof, "proof"}, {ErrPQAuthRequired, "pq_auth_required"}, {ErrNotPinned, "not_pinned"},
		{ErrCredentialsFailed, "credentials_failed"},
		{errProfile, "profile_disabled"}, {errMode, "record_mode"},
		{errEarly, "early_data"}, {errBinding, "binding_unavailable"}, {errHandle, "unknown_handle"},
		{errGate, "gate_error"}, {errCommit, "commit"},
	} {
		if errors.Is(err, c.err) {
			return c.name
		}
	}
	return "failed"
}
