package zmux_test

import (
	"net"
	"testing"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/adaptertest"
)

func TestNativeSessionContract(t *testing.T) {
	t.Parallel()

	clientRaw, serverRaw := net.Pipe()
	t.Cleanup(func() {
		_ = clientRaw.Close()
		_ = serverRaw.Close()
	})

	type result struct {
		session zmux.Session
		err     error
	}

	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)
	go func() {
		session, err := zmux.ClientSession(clientRaw, nil)
		clientCh <- result{session: session, err: err}
	}()
	go func() {
		session, err := zmux.ServerSession(serverRaw, nil)
		serverCh <- result{session: session, err: err}
	}()

	clientResult := <-clientCh
	serverResult := <-serverCh
	if clientResult.err != nil {
		t.Fatalf("ClientSession err = %v", clientResult.err)
	}
	if serverResult.err != nil {
		t.Fatalf("ServerSession err = %v", serverResult.err)
	}

	t.Cleanup(func() {
		_ = clientResult.session.Close()
		_ = serverResult.session.Close()
	})

	adaptertest.RunSessionContract(t, clientResult.session, serverResult.session)
}
