package zmux

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

func sealedTestFrame(n int) []byte {
	return append(binary.BigEndian.AppendUint16(nil, uint16(n+sealTag)), make([]byte, n+sealTag)...)
}

func TestNextBatchFSealedRecordReferences(t *testing.T) {
	frame := sealedTestFrame(1200)
	frame[2] = 1
	source := bytes.NewReader(frame)
	r := &sealedReader{lower: PipeIO{R: readerOnly{source}}, o: tagOpener{}}
	msg, err := r.ReadMessage(1200)
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Release()
	owner, body := msg.span.Buf, msg.Bytes()
	if len(body) != 1200 || owner == nil || len(owner.B) < len(body) || owner.Refs() != 1 {
		t.Fatalf("unexpected owned record length %d", len(body))
	}
	frame[2] = 2
	source.Reset(frame)
	second, err := r.ReadMessage(1200)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	body2 := second.Bytes()
	if body[0] != 1 || body2[0] != 2 {
		t.Fatal("an owned record was reused while referenced")
	}
	source.Reset(frame[:len(frame)-1])
	if _, err := readMessage(r, 1200); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short record returned %v", err)
	}
	r = &sealedReader{lower: PipeIO{R: readerOnly{source}}, o: rejectedRecord{}}
	source.Reset(frame)
	if _, err := readMessage(r, 1200); err == nil {
		t.Fatal("authentication failure returned a record")
	}
	pos := source.Len()
	if _, err := r.ReadMessage(1200); err == nil || source.Len() != pos {
		t.Fatal("authentication failure did not stop the record reader")
	}
}

type rejectedRecord struct{}

func (rejectedRecord) OpenInPlace([]byte) error {
	return errors.New("record authentication")
}

type shortSealedWrite struct{ calls int }

func (w *shortSealedWrite) Write(p []byte) (int, error) {
	w.calls++
	return len(p) - 1, nil
}
func (*shortSealedWrite) Close() error { return nil }

func TestNextBatchFSealedShortWriteStopsNonceSequence(t *testing.T) {
	w := &shortSealedWrite{}
	out := &sealedWriter{lower: PipeIO{W: w}, s: tagSealer{}}
	for range 2 {
		if err := out.WriteMessage([]byte("data")); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("sealed write returned %v", err)
		}
	}
	if w.calls != 2 {
		t.Fatalf("lower writer called %d times", w.calls)
	}
}

func BenchmarkNextBatchSealedRecord(b *testing.B) {
	frame := sealedTestFrame(1200)
	for _, pooled := range []bool{false, true} {
		name := "owned"
		if !pooled {
			name = "owned-by-caller"
		}
		b.Run(name, func(b *testing.B) {
			source := new(bytes.Reader)
			r := &sealedReader{lower: PipeIO{R: readerOnly{source}}, o: tagOpener{}}
			b.SetBytes(1200)
			b.ReportAllocs()
			for b.Loop() {
				source.Reset(frame)
				msg, err := r.ReadMessage(1200)
				if err != nil {
					b.Fatal(err)
				}
				if pooled {
					msg.Release()
				} else {
					msg.TakeBytes()
				}
			}
		})
	}
}

func TestSealedReceiveAllocations(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 4096, 4097, MaxRecordSize - 16} {
		frame := sealedTestFrame(size)
		source := new(bytes.Reader)
		r := &sealedReader{lower: PipeIO{R: readerOnly{source}}, o: tagOpener{}}
		n := testing.AllocsPerRun(100, func() {
			source.Reset(frame)
			msg, err := r.ReadMessage(size)
			if err != nil || msg.Len() != size {
				panic("invalid record")
			}
			msg.Release()
		})
		if n != 0 {
			t.Errorf("plaintext %d bytes: %g allocations per record", size, n)
		}
	}
}

func TestOldMessageTokenCannotReleaseNewGeneration(t *testing.T) {
	buf := core.NewBuf([]byte{1})
	releases := 0
	buf.Free = func(*core.Buf) { releases++ }
	first := ownMessageSpan(buf.Span(0, 1))
	stale := first
	first.Release()

	buf.B[0] = 2
	buf.Ref()
	second := ownMessageSpan(buf.Span(0, 1))
	stale.Release()
	if releases != 1 || second.Bytes()[0] != 2 || buf.Refs() != 1 {
		t.Fatal("stale message released a new owner")
	}
	second.Release()
	if releases != 2 {
		t.Fatalf("releases = %d", releases)
	}
}

type endingRecordReader struct {
	*bytes.Reader
	reads int
}

func (r *endingRecordReader) Read(p []byte) (int, error) {
	r.reads++
	n, err := r.Reader.Read(p)
	if r.Len() == 0 {
		err = io.EOF
	}
	return n, err
}

func (r *endingRecordReader) ReadMessage(limit int) (ReceivedMessage, error) {
	if r.Len() > limit {
		return ReceivedMessage{}, ErrMessageTooLarge
	}
	b := make([]byte, r.Len())
	_, err := r.Read(b)
	return OwnMessage(b), err
}

func (*endingRecordReader) Close() error { return nil }

func TestSealedReceiveTerminalRecord(t *testing.T) {
	for _, messages := range []bool{false, true} {
		frame := sealedTestFrame(1)
		frame[2] = 7
		input := &endingRecordReader{Reader: bytes.NewReader(frame)}
		if messages {
			input.Reset(frame[2:])
		}
		r := &sealedReader{lower: PipeIO{R: input}, o: tagOpener{}, msgs: messages}
		m, err := readMessage(r, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(m.Bytes(), []byte{7}) || !errors.Is(m.end, io.EOF) {
			t.Fatalf("messages=%t: record %v, end %v", messages, m.Bytes(), m.end)
		}
		m.Release()
		reads := input.reads
		if _, err := readMessage(r, 1); !errors.Is(err, io.EOF) || input.reads != reads {
			t.Fatalf("messages=%t: terminal read produced a record or read past EOF; err %v", messages, err)
		}
	}
}

func TestReplayedMessageKeepsItsReadEnd(t *testing.T) {
	input := &endingRecordReader{Reader: bytes.NewReader([]byte{1, 2})}
	p := PipeIO{R: input, Framing: FramingRecords}
	m, err := readMessage(p.R, 2)
	if err != nil {
		t.Fatal(err)
	}
	c := PipeConn(withFirstMessage(p, m))
	defer func() { _ = c.Close() }()
	var b [2]byte
	if _, err := io.ReadFull(c, b[:]); err != nil || b != [2]byte{1, 2} {
		t.Fatalf("replay read %v, err %v", b, err)
	}
	if n, err := c.Read(b[:]); n != 0 || !errors.Is(err, io.EOF) || input.reads != 1 {
		t.Fatalf("read after replay = %d, %v, carrier reads %d", n, err, input.reads)
	}
}

type terminalMessageCarrier struct {
	*msgCarrier
	end atomic.Bool
}

func (r *terminalMessageCarrier) ReadMessage(limit int) (ReceivedMessage, error) {
	m, err := r.msgCarrier.ReadMessage(limit)
	if err == nil && r.end.Swap(false) {
		err = io.EOF
	}
	return m, err
}

func TestMessageDriverConsumesRecordBeforeEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, peer := msgCarriers()
		defer func() { _ = local.Close() }()
		reader := &terminalMessageCarrier{msgCarrier: local}
		type result struct {
			conn *Conn
			err  error
		}
		started := make(chan result, 1)
		go func() {
			server, err := startServerNoListener(t.Context(), PipeIO{R: peer, W: peer, Framing: FramingRecords}, &Config{})
			started <- result{server, err}
		}()
		client, err := Start(t.Context(), PipeIO{R: reader, W: local, Framing: FramingRecords}, RoleClient, &Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		server := <-started
		if server.err != nil {
			t.Fatal(server.err)
		}
		defer func() { _ = server.conn.Close() }()
		synctest.Wait()
		count := client.ConnectionStats().RecordsReceived
		if count == 0 {
			t.Fatal("client did not receive the peer's parameters")
		}
		reader.end.Store(true)
		if err := peer.WriteMessage([]byte{byte(wire.TypePadding)}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if got := client.ConnectionStats().RecordsReceived; got != count+1 {
			t.Fatalf("records received = %d, want %d", got, count+1)
		}
	})
}

type suffixWriter struct {
	t     *testing.T
	first []byte
	at    int
}

func (w *suffixWriter) Write(p []byte) (int, error) {
	if w.first == nil {
		w.first = bytes.Clone(p)
	}
	if !bytes.Equal(p, w.first[w.at:]) {
		w.t.Fatal("ciphertext changed between short writes")
	}
	n := min(3, len(p))
	w.at += n
	return n, nil
}
func (*suffixWriter) Close() error { return nil }

type countingSealer struct {
	tagSealer
	calls int
}

func (s *countingSealer) Seal(dst, pt []byte) ([]byte, error) {
	s.calls++
	return s.tagSealer.Seal(dst, pt)
}

func TestSealedShortWritesKeepCiphertext(t *testing.T) {
	s := new(countingSealer)
	w := &suffixWriter{t: t}
	out := &sealedWriter{lower: PipeIO{W: w}, s: s}
	prev := 0
	err := out.writeMessages([][]byte{[]byte("payload")}, func(n int) {
		if n <= prev || n != w.at {
			t.Fatalf("reported prefix %d at %d", n, w.at)
		}
		prev = n
	})
	if err != nil || s.calls != 1 || w.at != len(w.first) || prev != w.at {
		t.Fatalf("seal calls %d, prefix %d/%d, err %v", s.calls, w.at, len(w.first), err)
	}
}
