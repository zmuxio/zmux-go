package zmuxh3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

func draft16Server(t *testing.T, config *quic.Config, handler func(*Server, http.ResponseWriter, *http.Request)) (*Transport, string) {
	t.Helper()
	srvTLS, cliTLS := tlsConfigs(t)
	packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{H3: &http3.Server{TLSConfig: srvTLS, QUICConfig: config}}
	srv.H3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler(srv, w, r) })
	tr := &Transport{TLSClientConfig: cliTLS}
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(packet) }()
	t.Cleanup(func() {
		_ = tr.Close()
		_ = srv.Close()
		_ = packet.Close()
		<-done
	})
	return tr, "https://" + packet.LocalAddr().String()
}

func draft16Peers(t *testing.T, ctx context.Context, config *quic.Config, before int) (*ClientConn, *Session, *Session) {
	t.Helper()
	accepted := make(chan *Session, 1)
	tr, u := draft16Server(t, config, func(s *Server, w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		session, err := s.Upgrade(w, r)
		if err == nil {
			checkWTAdvertisement(t, w.(http3.Settingser).Settings())
			accepted <- session
			<-session.Context().Done()
		}
	})
	req, authority, err := wtRequest(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	qc, err := quic.DialAddr(ctx, authority, tr.TLSClientConfig, wtQUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qc.CloseWithError(0, "") })
	cc, err := tr.NewClientConn(qc)
	if err != nil {
		t.Fatal(err)
	}
	for range before {
		get, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		response, err := cc.h3.RoundTrip(get)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("warm HTTP request returned %d", response.StatusCode)
		}
	}
	resp, local, err := cc.dial(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	select {
	case remote := <-accepted:
		return cc, local, remote
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil, nil, nil
	}
}

func checkWTAdvertisement(t *testing.T, settings *http3.Settings) {
	t.Helper()
	for _, key := range []uint64{0x2c7cf000, 0x2b603742, 0x2b603743, 0x14e9cd29, 0xc671706a} {
		if got := settings.Other[key]; got != 1 {
			t.Errorf("advertised SETTINGS %#x = %d, want 1", key, got)
		}
	}
}

func TestDraft16Session(t *testing.T) {
	for _, before := range []int{0, 64} {
		t.Run("CONNECT-"+strconv.Itoa(4*before), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cc, client, server := draft16Peers(t, ctx, nil, before)
			id := quic.StreamID(4 * before)
			if client.id != id || server.id != id {
				t.Fatalf("CONNECT IDs %d and %d, want %d", client.id, server.id, id)
			}
			checkWTAdvertisement(t, cc.h3.Settings())
			for _, peers := range [][2]*Session{{client, server}, {server, client}} {
				from, to := peers[0], peers[1]
				checkWebTransportStreamCredit(t, ctx, from, to)
				lane, err := from.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := lane.Write([]byte("lane")); err != nil {
					t.Fatal(err)
				}
				_ = lane.Close()
				in, err := to.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				deadline, _ := ctx.Deadline()
				_ = in.SetReadDeadline(deadline)
				if got, err := io.ReadAll(io.LimitReader(in, 5)); err != nil || string(got) != "lane" {
					t.Fatalf("receive lane: %q, %v", got, err)
				}
				if err := from.SendDatagram([]byte("payload")); err != nil {
					t.Fatal(err)
				}
				if got, err := to.ReceiveDatagram(ctx); err != nil || string(got) != "payload" {
					t.Fatalf("receive native datagram: %q, %v", got, err)
				}
				// Inject a wire-format HTTP Datagram, independently of the
				// session sender. CONNECT 256 uses a two-byte prefix.
				wire := quicvarint.Append(nil, uint64(id)/4)
				if err := from.conn.qc.SendDatagram(append(wire, "raw"...)); err != nil {
					t.Fatal(err)
				}
				if got, err := to.ReceiveDatagram(ctx); err != nil || string(got) != "raw" {
					t.Fatalf("receive RFC 9297 datagram: %q, %v", got, err)
				}
			}
			// Mix an unused flow control capsule with a drain notification.
			wire := quicvarint.Append(nil, 0x190b4d3d)
			wire = quicvarint.Append(append(wire, 1, 0), wtDrain)
			if _, err := client.control.Write(append(wire, 0)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-server.Draining():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			checkWebTransportStreamCredit(t, ctx, server, client)
			if err := client.CloseWithError(42, "done"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-server.Context().Done():
				if got, ok := errors.AsType[*SessionError](context.Cause(server.Context())); !ok ||
					*got != (SessionError{ErrorCode: 42, Message: "done", Remote: true}) {
					t.Fatalf("CONNECT ended with %v", context.Cause(server.Context()))
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := cc.conn.qc.Context().Err(); err != nil {
				t.Fatalf("borrowed QUIC connection closed with its CONNECT: %v", err)
			}
			req := server.conn.qc.LocalAddr()
			secondURL := "https://" + req.String()
			resp, next, err := cc.Dial(ctx, secondURL, nil)
			if resp != nil {
				_ = resp.Body.Close()
			}
			if next != nil || !errors.Is(err, errWTSessionLimit) {
				t.Fatalf("local refusal of second session: %v", err)
			}
			secondRequest, _, err := wtRequest(ctx, secondURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			second, err := cc.h3.OpenRequestStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer wtCancelControl(second)
			stop := context.AfterFunc(ctx, func() { wtCancelControl(second) })
			defer stop()
			if err := second.SendRequestHeader(secondRequest); err != nil {
				t.Fatal(err)
			}
			if resp, err := second.ReadResponse(); err == nil {
				_ = resp.Body.Close()
				t.Fatalf("server accepted second CONNECT: %v", resp)
			} else if e, ok := errors.AsType[*http3.Error](err); !ok || e.ErrorCode != http3.ErrCodeRequestRejected {
				t.Fatalf("remote refusal of second session: %v", err)
			}
		})
	}
}

func TestDraft16LegacyConnect(t *testing.T) {
	for _, tt := range []struct {
		name     string
		settings map[uint64]uint64
	}{
		{name: "no client setting"},
		{name: "legacy enable", settings: map[uint64]uint64{0x2b603742: 1, 0x2ac: 7}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tr, u := draft16Server(t, nil, func(s *Server, w http.ResponseWriter, r *http.Request) {
				sess, err := s.Upgrade(w, r)
				if err == nil {
					<-sess.Context().Done()
				}
			})
			req, authority, err := wtRequest(ctx, u, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Proto = "webtransport"
			qc, err := quic.DialAddr(ctx, authority, tr.TLSClientConfig, wtQUICConfig(&quic.Config{}))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = qc.CloseWithError(0, "") }()
			h3 := (&http3.Transport{EnableDatagrams: true, AdditionalSettings: tt.settings}).NewClientConn(qc)
			stream, err := h3.OpenRequestStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stop := context.AfterFunc(ctx, func() { wtCancelControl(stream) })
			defer stop()
			if err := stream.SendRequestHeader(req); err != nil {
				t.Fatal(err)
			}
			response, err := stream.ReadResponse()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("legacy CONNECT status %d", response.StatusCode)
			}
			_ = stream.Close()
		})
	}
}

func TestDraft16ResetFallback(t *testing.T) {
	for _, serverReliable := range []bool{true, false} {
		t.Run("peer-reliable-"+strconv.FormatBool(serverReliable), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, client, server := draft16Peers(t, ctx, &quic.Config{EnableStreamResetPartialDelivery: serverReliable}, 0)
			if client.state.SupportsStreamResetPartialDelivery.Remote != serverReliable {
				t.Fatal("incorrect negotiated reset support")
			}
			out, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var in *Stream
			if serverReliable {
				// Reset before peer acceptance. The reliable header must
				// still reach the peer and associate the stream.
				out.CancelWrite(42)
			}
			in, err = server.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !serverReliable {
				out.CancelWrite(42)
			}
			deadline, _ := ctx.Deadline()
			_ = in.SetReadDeadline(deadline)
			if n, err := in.Read(make([]byte, 1)); n != 0 {
				t.Fatalf("reset stream produced %d bytes", n)
			} else if reset, ok := errors.AsType[*StreamError](err); !ok || !reset.Remote || reset.ErrorCode != 42 {
				t.Fatalf("stream reset: %v", err)
			}
			in.CancelWrite(0)
			out.CancelRead(0)
			checkWebTransportStreamCredit(t, ctx, client, server)
		})
	}
}

func TestDraft16ConnectStatus(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusFound, http.StatusTooManyRequests} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tr, u := draft16Server(t, nil, func(_ *Server, w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusEarlyHints)
				w.WriteHeader(status)
			})
			session, err := DialSession(ctx, tr, u, nil, nil)
			if status == http.StatusNoContent {
				if err != nil || session == nil {
					t.Fatalf("2xx CONNECT: %v", err)
				}
				_ = session.CloseWithError(0, "")
			} else if e, ok := errors.AsType[*ConnectError](err); !ok || e.StatusCode != status || session != nil {
				t.Fatalf("CONNECT returned %v, want %d", err, status)
			}
		})
	}
}

func TestDraft16ReorderedStreamAndUnknownFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	accepted := make(chan *Session, 1)
	tr, u := draft16Server(t, nil, func(s *Server, w http.ResponseWriter, r *http.Request) {
		session, err := s.Upgrade(w, r)
		if err == nil {
			accepted <- session
			<-session.Context().Done()
		}
	})
	_, authority, err := wtRequest(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	qc, err := quic.DialAddr(ctx, authority, tr.TLSClientConfig, wtQUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	h3 := (&http3.Transport{EnableDatagrams: true, AdditionalSettings: map[uint64]uint64{wtEnabled: 1}}).NewClientConn(qc)
	select {
	case <-h3.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	deadline, _ := ctx.Deadline()
	connect, err := qc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = connect.SetDeadline(deadline)
	stream, err := qc.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.SetWriteDeadline(deadline)
	header := quicvarint.Append(nil, wtUni)
	header = quicvarint.Append(header, uint64(connect.StreamID()))
	if _, err := stream.Write(append(header, "early"...)); err != nil {
		t.Fatal(err)
	}
	_ = stream.Close()

	var headers bytes.Buffer
	encoder := qpack.NewEncoder(&headers)
	for _, field := range []qpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: authority},
		{Name: ":path", Value: "/"},
		{Name: ":protocol", Value: "webtransport-h3"},
	} {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	// The reserved GREASE frame type precedes valid CONNECT HEADERS.
	wire := quicvarint.Append(nil, 0x21)
	wire = append(wire, 1, 0xff, 1)
	wire = quicvarint.Append(wire, uint64(headers.Len()))
	if _, err := connect.Write(append(wire, headers.Bytes()...)); err != nil {
		t.Fatal(err)
	}
	select {
	case session := <-accepted:
		in, err := session.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = in.SetReadDeadline(deadline)
		if got, err := io.ReadAll(io.LimitReader(in, 6)); err != nil || string(got) != "early" {
			t.Fatalf("reordered lane: %q, %v", got, err)
		}
		_ = connect.Close()
		select {
		case <-session.Context().Done():
			if closed, ok := errors.AsType[*SessionError](context.Cause(session.Context())); !ok ||
				*closed != (SessionError{Remote: true}) {
				t.Fatalf("clean CONNECT FIN: %v", context.Cause(session.Context()))
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
