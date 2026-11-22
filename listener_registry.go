package zmux

import (
	"hash/maphash"
	"slices"
	"sync"
)

// Session IDs remain stable over locator rotations. Each locator has its
// own index shard so a lookup needs only the locator on the wire.
const registryShards = 64

type sessionRegistry struct {
	live [registryShards]liveShard
	loc  [registryShards]locatorShard
	seed maphash.Seed
}

type liveShard struct {
	sessions map[*Conn]bool // under mu
	mu       sync.Mutex
}

type locatorShard struct {
	sessions map[string]*Conn // under mu
	mu       sync.RWMutex
}

func (r *sessionRegistry) owner(c *Conn) *liveShard {
	return &r.live[uint64(c.id)%registryShards]
}

func (r *sessionRegistry) index(k string) *locatorShard {
	return &r.loc[maphash.String(r.seed, k)%registryShards]
}

func (r *sessionRegistry) lookup(k string) *Conn {
	sh := r.index(k)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.sessions[k]
}

func (r *sessionRegistry) bind(c *Conn, k string) {
	sh := r.index(k)
	sh.mu.Lock()
	if sh.sessions == nil {
		sh.sessions = make(map[string]*Conn)
	}
	sh.sessions[k] = c
	sh.mu.Unlock()
	if !slices.Contains(c.lstLocs, k) {
		c.lstLocs = append(c.lstLocs, k)
	}
}

func (r *sessionRegistry) unbind(c *Conn, k string) {
	sh := r.index(k)
	sh.mu.Lock()
	if sh.sessions[k] == c {
		delete(sh.sessions, k)
	}
	sh.mu.Unlock()
}

func (l *Listener) register(c *Conn, locator []byte) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.stopCtx.Err() != nil {
		return false
	}
	r := &l.registry
	sh := r.owner(c)
	sh.mu.Lock()
	if sh.sessions == nil {
		sh.sessions = make(map[*Conn]bool)
	}
	sh.sessions[c] = false // provisional until backlog publication
	if len(locator) > 0 {
		r.bind(c, string(locator))
	}
	sh.mu.Unlock()
	return true
}

// publish marks an established bootstrap eligible for the Shutdown snapshot.
func (r *sessionRegistry) publish(c *Conn) bool {
	sh := r.owner(c)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.sessions[c]; !ok {
		return false
	}
	sh.sessions[c] = true
	return true
}
func (r *sessionRegistry) snapshot(publishedOnly bool) (cs []*Conn) {
	for i := range r.live {
		sh := &r.live[i]
		sh.mu.Lock()
		for c, published := range sh.sessions {
			if published || !publishedOnly {
				cs = append(cs, c)
			}
		}
		sh.mu.Unlock()
	}
	return cs
}

// sessionClosed detaches the membership under its shard, then drops epoch
// locators independently. Both normal close and panic salvage may call it.
func (l *Listener) sessionClosed(c *Conn) {
	c.unregister.Do(func() {
		r := &l.registry
		locs := r.detach(c)
		for _, k := range locs {
			r.unbind(c, k)
		}
		if c.inSusp.Load() {
			l.suspMu.Lock()
			l.dropSuspended(c)
			l.suspMu.Unlock()
		}
	})
}

func (l *Listener) addLocator(c *Conn, loc []byte) {
	r := &l.registry
	sh := r.owner(c)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.sessions[c]; ok && l.stopCtx.Err() == nil {
		r.bind(c, string(loc))
	}
}

func (l *Listener) removeLocator(c *Conn, loc []byte) {
	r := &l.registry
	sh := r.owner(c)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(loc)
	r.unbind(c, k)
	c.lstLocs = slices.DeleteFunc(c.lstLocs, func(x string) bool { return x == k })
}

// detach removes membership before index cleanup. A later binding of a
// locator can safely race its former owner's unbind.
func (r *sessionRegistry) detach(c *Conn) []string {
	sh := r.owner(c)
	sh.mu.Lock()
	delete(sh.sessions, c)
	locs := c.lstLocs
	c.lstLocs = nil
	sh.mu.Unlock()
	return locs
}
