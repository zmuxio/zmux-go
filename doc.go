// Package zmux multiplexes streams and datagrams over one or many reliable
// byte pipes. It speaks QMux (so a plain QMux peer interoperates) and, with
// a zmux peer, adds stream preambles, priorities, acknowledged pipes,
// multipath with failover and resume, lanes over multi-stream carriers,
// and nested sessions.
//
// # Sessions and streams
//
// Dial and Server start a session over a net.Conn; Start takes any
// carrier (PipeIO) and an explicit Role. A Listener accepts sessions and
// the pipes that join them (HandlePipe, or ServeHTTP for HTTP carriers).
// The Conn and Stream methods follow quic-go: OpenStream[Sync],
// AcceptStream, Read, Write, Close (FIN), CancelRead, CancelWrite,
// deadlines, SendDatagram and ReceiveDatagram.
//
// Blocking: calls that wait take a context.Context (OpenStreamSync,
// AcceptStream, Ping, Shutdown, AddPipe, ...) or honour the stream's
// deadlines (Read, Write). Close, CloseWithError, CancelRead, CancelWrite
// and Pipe.Close never block; Wait blocks until the session and all its
// goroutines ended. Conn.Context and Stream.Context are the session's
// context.
//
// Ownership: a Stream handle that is garbage-collected without being
// closed resets and stops its stream, so keep it while it is in use. A
// PipeIO given to Start, AddPipe or HandlePipe belongs to zmux from then
// on, which closes each half exactly once, unless the call fails with
// ErrInvalidConfig before taking it.
// A Chunk from ReadChunk is released exactly once, by Release or by
// WriteChunk.
//
// # Errors
//
// A session's end is one of *ApplicationError, *TransportError,
// *IdleTimeoutError, *HandshakeTimeoutError, *EarlyDataRejectedError,
// *ResumeTimeoutError or *PipesLostError; every one matches net.ErrClosed
// with errors.Is, and the timeouts also os.ErrDeadlineExceeded. A stream
// the peer reset or stopped gives a *StreamError. IsRetryable reports
// streams the peer did not process (ErrRejected, ErrGoingAway, a rejected
// early-data session), which can be sent again. Other sentinel errors
// (ErrStreamClosed, ErrTooLate, ErrNotNegotiated, ErrInvalidConfig, ...)
// are documented where they are returned.
//
// # Configuration
//
// The zero value of every Config field selects its default. For sizes,
// counts and durations that can be turned off or unlimited, a negative
// value means off or unlimited (MaxIdleTimeout, MemoryBudget,
// MaxIncomingStreams, ListenerConfig.MemoryBudget); pointer fields
// distinguish "unset" where zero is itself meaningful. Multipath needs
// MaxPipes above 1 and a PipeAuth (PSK or TLS); without a secret, joins
// are possible only with PipeAuth.AllowInsecure, meant for tests and
// protected networks.
package zmux
