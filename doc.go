// Package zmux multiplexes streams and datagrams over one or many reliable
// byte pipes. It speaks QMux (so a plain QMux peer interoperates) and, with
// a zmux peer, adds stream preambles, priorities, acknowledged pipes,
// multipath with failover and resume, lanes over multi-stream carriers,
// and nested sessions.
//
// # Sessions and streams
//
// A client dials with Dial (TCP) or DialTLS, or starts a session on a
// connection it already has with Client. A server serves a net.Listener
// with a Listener (Listener.Serve) and takes the sessions from
// Listener.Accept; the Listener also accepts the pipes that join its
// sessions, so it is the server for multipath and resume. Server starts a
// single-pipe session on one accepted connection. Start takes any carrier
// (PipeIO, for example a WebSocket connection with FramingRecords) and an
// explicit Role, and Listener.HandlePipe or NewHTTPHandler accept any
// carrier. An Admission on the Listener (ListenerConfig.Admission,
// PrefixAuth) decides on every carrier before anything is sent on it,
// whatever entry point took it, and a Credential is what a
// client presents for it (Config.Credential, PresentPipe). HTTPExchangePipe and HTTPExchangeHandler build a carrier out of
// short HTTP requests for paths that buffer or cut long ones; they
// authenticate but do not encrypt, so a proxy or CDN that terminates TLS
// can read such a carrier. The sub-modules zmuxquic and zmuxh3 carry
// sessions over QUIC, HTTP/3 and WebTransport.
//
// The Conn and Stream methods follow quic-go: OpenStream[Sync],
// AcceptStream, Read, Write, Close (FIN), CancelRead, CancelWrite,
// deadlines, SendDatagram and ReceiveDatagram. StreamConn gives a stream
// the net.Conn interface.
//
// Blocking: calls that wait take a context.Context (OpenStreamSync,
// AcceptStream, Ping, Shutdown, AddPipe, NextEvent, ...) or honour the
// stream's deadlines (Read, Write). Close, CloseWithError, CancelRead,
// CancelWrite and Pipe.Close never block; Wait blocks until the session
// and all its goroutines ended. Conn.Context and Stream.Context are the
// session's context.
//
// Closing: Conn.Close ends the session at once and discards data not yet
// sent; Conn.Shutdown lets open streams finish first. Listener.Shutdown
// does the same for every session of a listener.
//
// Ownership: a Stream handle that is garbage-collected without being
// closed resets and stops its stream, so keep it while it is in use. A
// net.Conn given to Client or Server and a PipeIO given to Start, AddPipe
// or HandlePipe belong to zmux from then on, which closes each half
// exactly once, unless the call fails with ErrInvalidConfig before taking
// it. A Chunk from ReadChunk is released exactly once, by Release or by
// WriteChunk.
//
// # Events
//
// Pipe changes and other session events (EventKind) can be read with
// Conn.NextEvent, or received with a Config.Tracer; both are optional and
// bounded, and the current state is always available from Conn.PipeStats
// and Conn.ConnectionState.
//
// # Errors
//
// A session's end is one of *ApplicationError, *TransportError,
// *IdleTimeoutError, *HandshakeTimeoutError, *EarlyDataRejectedError,
// *ResumeTimeoutError or *PipesLostError. Every one matches net.ErrClosed
// with errors.Is; the last five also match their sentinel
// (ErrIdleTimeout, ErrHandshakeTimeout, ErrEarlyDataRejected,
// ErrResumeTimeout, ErrPipesLost), the timeouts also
// os.ErrDeadlineExceeded, and the two that a lost pipe caused unwrap to
// that pipe's error. A stream the peer reset or stopped gives a
// *StreamError. IsRetryable reports streams the peer did not process
// (ErrRejected, ErrGoingAway, ErrEarlyDataRejected), which can be sent
// again. Calls fail with ErrStreamClosed, ErrNotNegotiated,
// ErrNotEstablished, ErrTooLate, ErrInvalidConfig, ErrPipeGone or
// ErrNoDecision where their documentation says so.
//
// # Names
//
// Constants carry the name of their type, or a short form of it, as a
// prefix: Role (RoleClient), Direction (DirSendOnly), Framing
// (FramingRecords), Urgency (UrgencyDefault), EventKind (EventPipeJoined),
// OutboundState (OutboundActive), Endpoint (EndpointLocal), Transport for
// transport error codes (TransportProtocolViolation) and Stream for stream
// error codes (StreamCancelled).
//
// Rates are float64 bytes per second throughout (PickPipe.Rate,
// PipeStats.DeliveryRate, Event.DeliveryRate, CapacityFloor.Rate); memory
// budgets and usage are int64 bytes.
//
// # Configuration
//
// Settings: the zero value of a field selects its default, stated in the
// field's documentation; for timeouts and limits it is a safe bound unless
// the field says otherwise. A negative timeout means no timeout, as for
// http.Server.IdleTimeout. Other negative values are an error unless the
// field gives them a meaning.
//
// The fields that give negative values a meaning follow the standard
// library: KeepAlivePeriod turns keep-alive off (net.Dialer.KeepAlive),
// SchedulerParams.ProbeInterval and StandbyProbe and
// PipeOptions.ProbeInterval turn probes off (net.KeepAliveConfig), and
// HTTPExchangeConfig.Coalesce sends at once
// (httputil.ReverseProxy.FlushInterval). A memory budget is lifted with
// NoMemoryLimit. Other settings that turn something off are Disable and
// No fields, as in net/http (NoIncomingStreams, NoProbeJitter,
// PoolDecision.DisableIdleProbe, QualityBand.NoMinInterval,
// shaping.Limits.NoPadding, ...). Zero values that are not a safe bound:
// shaping.Limits (zero is unbounded; the session's other limits still
// apply), PipePool.Rotate (zero rotates nothing) and Config.ResumeTimeout
// (zero is no resume). Errors are ErrInvalidConfig; NewListener panics.
//
// Multipath needs
// MaxPipes above 1 and a PipeAuth (PSK or TLS); without a secret, joins
// are possible only with PipeAuth.AllowInsecure, meant for tests and
// protected networks.
//
// # Time
//
// Config.Clock (a timeshim.Clock; nil is the host's monotonic clock) is the
// session's time source: its timers, deadlines, RTT and rate estimates, and
// the Redial and MaintainPipes waits run on it, so a timeshim.Fake drives a
// session deterministically. Carrier read and write deadlines of
// net.Conn, Listener and endpoint timers (first record, pairing, carrier
// upgrade, the exchange carrier) and the transports of the QUIC adapters
// (zmuxquic, zmuxh3: quic-go's loss recovery, idle and handshake timers)
// stay on the host clock.
package zmux
