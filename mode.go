package zmux

// CarrierKind names a kind of carrier.
type CarrierKind uint8

//goland:noinspection GoUnusedConst // the adapter modules use their kinds
const (
	// CarrierWebSocket is a WebSocket (zmuxws.Candidate).
	CarrierWebSocket CarrierKind = iota + 1
	// CarrierStream is a bidirectional streaming request: one request
	// whose body is the uplink and whose response body the downlink
	// (zmuxhttp.Streaming.Candidate).
	CarrierStream
	// CarrierSplit is a split bootstrap: an uplink POST and a downlink GET
	// (zmuxhttp.Split.Candidate).
	CarrierSplit
	// CarrierExchange is the exchange carrier of bounded requests
	// (zmuxhttp.Exchange.Candidate).
	CarrierExchange
	// CarrierQUIC is a QUIC stream (zmuxquic.Candidate), direct only.
	CarrierQUIC
	// CarrierHTTP3 is a streaming request over HTTP/3
	// (zmuxh3.HTTP3Candidate).
	CarrierHTTP3
	// CarrierWebTransport is a WebTransport stream
	// (zmuxh3.WebTransportCandidate), direct only.
	CarrierWebTransport
	// CarrierRaw is a reliable ordered byte connection that carries the
	// records itself, with no carrier framing of its own: TCP, TLS, a Unix
	// socket, a stream of another session (NetPipe, SecureTLS, StreamPipe).
	CarrierRaw
)

var carrierKindNames = [...]string{"", "websocket", "streaming", "split", "exchange", "quic", "http3", "webtransport", "raw"}

// String returns the kind's name: "websocket", "streaming", "split",
// "exchange", "quic", "http3", "webtransport" or "raw".
func (k CarrierKind) String() string {
	if int(k) < len(carrierKindNames) && k != 0 {
		return carrierKindNames[k]
	}
	return "CarrierKind(?)"
}

// Candidate registers a carrier opener and its capabilities in one domain.
type Candidate struct {
	Open        CarrierOpener
	ID          CandidateID
	CacheKey    string
	Variant     string
	Attachments []AttachmentSpec
	Tier        uint64
	Kind        CarrierKind
	Standby     bool
	CanStart    bool
}
