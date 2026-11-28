// Package testsession provides in-process session fixtures for adapter tests.
package testsession

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// FirstRecord returns the first carrier write from the supplied configuration.
func FirstRecord(t *testing.T, cfg *zmux.Config) []byte {
	t.Helper()
	client, peer := net.Pipe()
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer func() {
		cancel()
		_ = peer.Close()
		<-done
	}()
	go func() {
		defer close(done)
		conn, _ := zmux.Start(ctx, zmux.NetPipe(client), zmux.RoleClient, cfg)
		if conn != nil {
			conn.Abort(0, "")
			_ = conn.Wait()
		}
	}()
	// Watchdog for a test that fails to produce any first flight.
	if err := peer.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	first := make([]byte, zmux.MaxRecordSize+8)
	n, err := peer.Read(first)
	if err != nil {
		t.Fatal(err)
	}
	return first[:n:n]
}
