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
// explicit Role, and Listener.HandlePipe or zmuxhttp.NewHandler accept any
// carrier. An Admission on the Listener (ListenerConfig.Admission,
// admit.PrefixAuth) decides on every carrier before anything is sent on it,
// whatever entry point took it, and a Credential is what a
// client presents for it (Config.Credential, PresentPipe).
//
// Packages beside this one: zmuxhttp carries sessions over HTTP requests
// (stream, split and exchange carriers; the exchange carrier, of short
// requests for paths that buffer or cut long ones, authenticates but does
// not encrypt, so a proxy or CDN that terminates TLS can read it); admit
// holds what a server judges a carrier by before any handshake
// (admit.ClassifyFirstBytes, admit.PrefixAuth, admit.ReplayFilter);
// carrier holds the building blocks of carrier adapters (a connection
// pool, a channel datagram demultiplexer). The sub-modules zmuxquic and
// zmuxh3 carry sessions over QUIC, HTTP/3 and WebTransport, and zmuxws
// over WebSocket.
//
// A carrier transform (a byte transform, compression, a handshake of its
// own) wraps carriers with the public API: a Candidate's Dial on the
// client, ListenerConfig.WrapPipe on the server, behind an Admission that
// tells its first bytes from a zmux first record (admit.ClassifyFirstBytes,
// VerdictMore, Decision.Variant). Its halves offer the optional
// capabilities that PipeIO lists (KeyingMaterialExporter for its own
// exporter, EarlyClientCarrier for a first flight that the server may
// refuse, with EarlyData.HoldFirstFlight and Conn.ReleaseFirstFlight);
// admit.ReplayFilter is a single-use cache for its nonces.
//
// The Conn and Stream methods follow quic-go: OpenStream[Sync],
// AcceptStream, Read, Write, Close (FIN), CancelRead, CancelWrite,
// deadlines, SendDatagram and ReceiveDatagram. StreamConn gives a stream
// the net.Conn interface.
//
// Blocking: calls that wait take a context.Context (OpenStreamSync,
// AcceptStream, Ping, Shutdown, AddPipe, NextEvent, ...) or honour the
// stream's deadlines (Read, Write). Conn.Close blocks only until the
// written data is flushed and CONNECTION_CLOSE has been handed to the
// carriers (at most one second); Conn.Abort, CancelRead, CancelWrite and Pipe.Close never block; Wait
// blocks until the session and all its goroutines ended. Conn.Context and Stream.Context are the
// session's context.
//
// Closing: Conn.Close flushes what the streams have written and ends the
// session, and a process may exit as soon as it returns; Conn.Abort ends
// it at once with an error code, discarding data not yet sent;
// Conn.Shutdown lets open streams finish first. Listener.Shutdown
// does the same for every session of a listener.
//
// Ownership: a Stream handle that is garbage-collected without being
// closed resets and stops its stream, so keep it while it is in use. A
// net.Conn given to Client, Server or a Dial function and a PipeIO given to
// Start, AddPipe or HandlePipe belong to zmux from then on, whether the
// call succeeds or fails (ErrInvalidConfig, a refused or failed join, a
// panic in a host callback included): zmux closes each half exactly once,
// and the caller never closes it. A Chunk from ReadChunk is released exactly once, by Release or by
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
// prefix: Role (RoleClient), Direction (DirectionSendOnly), Framing
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
// Settings: each setting is written one way. The zero value of a field
// selects its default, stated in the field's documentation; for timeouts
// and limits it is a safe bound unless the field says otherwise. A negative
// value is the field's off, none, no timeout or unlimited setting where the
// field has one, following the standard library (net.Dialer.KeepAlive,
// http.Server.IdleTimeout): Liveness.KeepAlivePeriod, StandbyProbe and
// ProbeInterval turn keep-alive and probes off, a negative ProbeJitter is
// a jitter of 0, a negative MaxIncomingStreams grants no stream, a negative
// MemoryBudget is unlimited, a negative DatagramStreamBytes leaves a
// stream's datagram queue without a byte bound, a negative
// SchedulerParams.Probation turns
// probation off and a negative zmuxhttp.ExchangeConfig.Coalesce sends at once.
// A negative value of a field without such a setting (JoinTimeout,
// ResumeTimeout, the windows, MaxPipes) is an error. A feature that is on
// by default is turned off with a Switch field set to SwitchOff
// (EpochAfterFirstPipe, SchedulerParams.Redundancy, ...); one that is off
// by default is a bool named for it (EnableDatagrams, ReturnOnReply).
// Zero values that are not a safe bound: shaping.Limits (zero is
// unbounded; the session's other limits still apply), PipePool.Rotate
// (zero rotates nothing) and Config.ResumeTimeout (zero is no resume). A
// value out of range is an error, never corrected. Errors are
// ErrInvalidConfig, from Start, Dial, Client and Server before a byte is
// written, and from NewListener.
//
// Multipath needs MaxPipes above 1 and a PipeAuth (PSK, DeferredPSK with
// Conn.SetJoinSecret, or Exporter); a session whose first pipe is not on the secure
// carrier has join keys only once its host declared that pipe end to end
// (PipeAuth.EndToEnd or Conn.DeclareEndToEnd). Every added pipe uses the Join
// handshake. A process restored from a snapshot MUST reseed its random source before
// any Join: repeated random output can repeat a Reply's nonce, which replays an old
// confirmation and reuses record keys.
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
