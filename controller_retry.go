package zmux

import (
	"maps"
	"sync"
	"time"
)

// dueStarts records one suppression for each candidate in a start
// opportunity. Resource notifications can recheck an opportunity without
// opening a new one. A timed retry can become independently due.
type dueStarts struct {
	mu      sync.Mutex
	blocked map[CarrierCacheKey]time.Time
	plan    retryPlan
}

type retryPlan struct {
	revision uint64
	need     Directions
	group    GroupID
	quota    QuotaID
	upgrade  CandidateID
}

func (m *dueStarts) prepare(plan reconcilePlan) {
	key := retryPlan{revision: plan.revision, need: plan.need, upgrade: plan.upgrade}
	if plan.group != nil {
		key.group = plan.group.ID
	}
	if plan.quota != nil {
		key.quota = plan.quota.ID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.plan != key {
		m.plan = key
		clear(m.blocked)
	}
}

func (m *dueStarts) transfer(next *dueStarts, revision uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.plan.need == 0 {
		next.plan = m.plan
		next.plan.revision = revision
		next.blocked = maps.Clone(m.blocked)
	}
}

func (m *dueStarts) started(key CarrierCacheKey) {
	m.mu.Lock()
	delete(m.blocked, key)
	m.mu.Unlock()
}

func (m *dueStarts) recovery() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.plan.need != 0
}

func (m *dueStarts) suppress(cache *CarrierCache, key CarrierCacheKey, retryAt time.Time, err error) {
	reason := retrySuppression(err)
	if reason == "" {
		return
	}
	m.mu.Lock()
	if _, ok := m.blocked[key]; ok {
		m.mu.Unlock()
		return
	}
	if m.blocked == nil {
		m.blocked = make(map[CarrierCacheKey]time.Time)
	}
	m.blocked[key] = retryAt
	m.mu.Unlock()
	cache.suppress(key, reason)
}

func (ctl *SessionController) suppressStart(demand *controllerDemand, key CarrierCacheKey, wake time.Time, err error) {
	if demand.session != nil && demand.starts.recovery() {
		ctl.cache.suppressRecovery(key, demand.session, retrySuppression(err))
		return
	}
	demand.starts.suppress(ctl.cache, key, wake, err)
}
