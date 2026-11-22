// Package carrierstats shares lazy native observations across a connection's users.
package carrierstats

import (
	"reflect"
	"runtime"
	"sync"
	"time"
	"weak"
)

type Source string

const (
	SourceUnknown                 Source = "unknown"
	SourceLinuxTCPInfo            Source = "linux_tcp_info"
	SourceDarwinTCPConnectionInfo Source = "darwin_tcp_connection_info"
	SourceWindowsSIOTCPInfo       Source = "windows_sio_tcp_info"
	SourceQUICGo                  Source = "quic_go"
)

type CarrierID struct{ serial uint64 }
type Direction string

const Outbound Direction = "outbound"

type Readout struct {
	Sent, Lost uint64
	Known      bool
}

type Observation struct {
	At     time.Time
	Source Source
	Readout
	ID         CarrierID
	Generation uint64
}

// Sampler serializes native reads and publishes the pair with its metadata.
type Sampler struct {
	mu        sync.Mutex
	read      func() Readout
	last      Observation
	baseline  Readout
	source    Source
	inFlight  bool
	firstDone chan struct{}
}

var registry struct {
	caches map[weak.Pointer[byte]]cacheEntry
	sync.Mutex
	next uint64
}

type cacheEntry struct {
	sampler *Sampler
	source  Source
}

func cache(conn *byte, source Source, makeRead func() func() Readout) *Sampler {
	key := weak.Make(conn)
	registry.Lock()
	defer registry.Unlock()
	if entry, ok := registry.caches[key]; ok {
		if entry.source == SourceUnknown && source != SourceUnknown {
			entry.sampler.mu.Lock()
			entry.sampler.read = makeRead()
			entry.sampler.source = source
			entry.sampler.last = Observation{ID: entry.sampler.last.ID, Source: source}
			entry.sampler.mu.Unlock()
			registry.caches[key] = cacheEntry{entry.sampler, source}
		}
		return entry.sampler
	}
	if registry.caches == nil {
		registry.caches = make(map[weak.Pointer[byte]]cacheEntry)
	}
	registry.next++
	sampler := &Sampler{read: makeRead(), source: source, last: Observation{
		ID: CarrierID{serial: registry.next}, Source: source,
	}}
	registry.caches[key] = cacheEntry{sampler, source}
	runtime.AddCleanup(conn, func(key weak.Pointer[byte]) {
		registry.Lock()
		delete(registry.caches, key)
		registry.Unlock()
	}, key)
	return sampler
}

func For[T any](conn *T, source Source, read func(*T) Readout) *Sampler {
	if conn == nil {
		return nil
	}
	pointer := weak.Make(conn)
	return cache((*byte)(reflect.ValueOf(conn).UnsafePointer()), source, func() func() Readout {
		return func() Readout {
			if conn := pointer.Value(); conn != nil {
				return read(conn)
			}
			return Readout{}
		}
	})
}

// ForQUIC registers the shared packet counters for a native connection.
//
//goland:noinspection GoUnusedExportedFunction // used by the QUIC and HTTP/3 adapter modules
func ForQUIC[T any](conn *T, packets func(*T) (sent, lost uint64)) *Sampler {
	return For(conn, SourceQUICGo, func(c *T) Readout {
		sent, lost := packets(c)
		return Readout{Sent: sent, Lost: lost, Known: true}
	})
}

// Unknown tracks a concrete pointer identity with no native provider.
func Unknown(conn any) *Sampler {
	value := reflect.ValueOf(conn)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Type().Elem().Size() == 0 {
		return nil
	}
	return cache((*byte)(value.UnsafePointer()), SourceUnknown, func() func() Readout {
		return func() Readout { return Readout{} }
	})
}

// Prime acquires the first sample at attachment. Concurrent attachments
// wait for that same sample. Public readers never wait.
func Prime(s *Sampler) {
	s.prime(time.Now)
}

func (s *Sampler) prime(now func() time.Time) {
	for {
		s.mu.Lock()
		if !s.last.At.IsZero() {
			s.mu.Unlock()
			return
		}
		if s.inFlight {
			if s.firstDone == nil {
				s.firstDone = make(chan struct{})
			}
			done := s.firstDone
			s.mu.Unlock()
			<-done
			continue
		}
		s.inFlight = true
		read, source := s.read, s.source
		s.mu.Unlock()
		s.query(now, read, source)
	}
}

// Cached returns the last publication without scheduling a query.
func Cached(s *Sampler) Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func Sample(s *Sampler) Observation {
	return s.sample(time.Now, func(query func()) { go query() })
}

func (s *Sampler) sample(now func() time.Time, start func(func())) Observation {
	s.mu.Lock()
	last := s.last
	if s.inFlight || !last.At.IsZero() && (s.source == SourceWindowsSIOTCPInfo ||
		s.source == SourceUnknown || now().Sub(last.At) < time.Second) {
		s.mu.Unlock()
		return last
	}
	s.inFlight = true
	read, source := s.read, s.source
	s.mu.Unlock()
	start(func() { s.query(now, read, source) })
	return last
}

func (s *Sampler) query(now func() time.Time, read func() Readout, source Source) {
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.inFlight = false
		if s.firstDone != nil {
			close(s.firstDone)
			s.firstDone = nil
		}
	}()
	next := read()
	at := now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if source != s.source {
		return // The provider was superseded during the query.
	}
	s.last.Source = source
	if next.Known {
		raw := next
		if s.baseline.Known {
			switch s.last.Source {
			case SourceLinuxTCPInfo:
				var sentOK, lostOK bool
				next.Sent, sentOK = extend(s.baseline.Sent, raw.Sent)
				next.Lost, lostOK = extend(s.baseline.Lost, raw.Lost)
				if !sentOK || !lostOK {
					s.last.Generation++
					next = raw
				}
			case SourceDarwinTCPConnectionInfo, SourceQUICGo:
				if raw.Sent < s.baseline.Sent ||
					s.last.Source == SourceDarwinTCPConnectionInfo && raw.Lost < s.baseline.Lost {
					s.last.Generation++
				}
			case SourceUnknown, SourceWindowsSIOTCPInfo:
				// These sources have no native pair.
			}
		}
		s.baseline = next
	}
	s.last.At, s.last.Readout = at, next
}

// Extend a wrap only when a rollback crosses the 32-bit boundary with
// both values in the adjacent quarters. Other rollbacks are ambiguous.
func extend(previous, raw uint64) (uint64, bool) {
	const span = 1 << 32
	low := previous % span
	high := previous - low
	if raw >= low {
		return high + raw, true
	}
	if low >= 3*span/4 && raw < span/4 {
		return high + span + raw, true
	}
	return raw, false
}

// Provider is the capability forwarded by sending halves.
type Provider interface {
	CarrierSampler() *Sampler
}

func ProviderOf(half any) *Sampler {
	if p, ok := half.(Provider); ok {
		return p.CarrierSampler()
	}
	return nil
}
