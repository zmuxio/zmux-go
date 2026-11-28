package streamio

import (
	"errors"
	"io"
	"net"
	"testing"
)

type hookedStream struct {
	write func([]byte) (int, error)
	close func() error
}

func (s hookedStream) Write(p []byte) (int, error) { return s.write(p) }
func (s hookedStream) Close() error                { return s.close() }

func TestNextBatchTransportWriteOwnership(t *testing.T) {
	var w Writer
	started, canceled := make(chan struct{}), make(chan struct{})
	ord := make(chan string, 3)
	str := hookedStream{
		write: func([]byte) (int, error) {
			ord <- "write"
			close(started)
			<-canceled
			return 1, net.ErrClosed
		},
		close: func() error {
			ord <- "fin"
			return nil
		},
	}
	writeDone := make(chan error, 1)
	go func() { _, err := w.Write(str, []byte{1, 2}); writeDone <- err }()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close(str) }()
	stop := func() {
		ord <- "reset"
		close(canceled)
	}
	w.Abort(stop)
	w.Abort(stop)
	if err := <-writeDone; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("blocked write: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	close(ord)
	var events []string
	for event := range ord {
		events = append(events, event)
	}
	if len(events) != 2 || events[0] != "write" || events[1] != "reset" {
		t.Fatalf("write/close order: %v", events)
	}
}

func TestNextBatchTransportShortWrite(t *testing.T) {
	var w Writer
	str := hookedStream{write: func([]byte) (int, error) { return 1, nil }}
	if n, err := w.Write(str, []byte{1, 2}); n != 1 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write = %d, %v", n, err)
	}
}
