package store

import (
	"context"
	"sort"
	"time"
)

type healthKey struct {
	tenantID string
	bucket   time.Time
}

func (m *Memory) RecordIngestHealth(_ context.Context, samples []IngestSample) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range samples {
		key := healthKey{tenantID: s.TenantID, bucket: s.Bucket.UTC().Truncate(time.Minute)}
		cur := m.health[key]
		cur.Received += s.Received
		cur.Dropped += s.Dropped
		cur.HandlerErrors += s.HandlerErrors
		m.health[key] = cur
	}
	return nil
}

func (m *Memory) HasIngestGap(_ context.Context, tenantID string, from, to time.Time) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hasGapLocked(tenantID, from, to), nil
}

// hasGapLocked is the lock-free body, callable from ClaimExpired which already
// holds the write lock.
func (m *Memory) hasGapLocked(tenantID string, from, to time.Time) bool {
	lo := from.UTC().Truncate(time.Minute)
	hi := to.UTC().Truncate(time.Minute)

	for key, v := range m.health {
		if key.tenantID != tenantID {
			continue
		}
		if key.bucket.Before(lo) || key.bucket.After(hi) {
			continue
		}
		if v.Dropped > 0 || v.HandlerErrors > 0 {
			return true
		}
	}
	return false
}

func (m *Memory) SilentTenants(_ context.Context, now time.Time, params SilenceParams) ([]string, error) {
	if params.Quiet <= 0 || params.Baseline <= 0 || params.MinActiveBuckets <= 0 {
		return nil, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	quietFrom := now.UTC().Add(-params.Quiet).Truncate(time.Minute)
	baselineFrom := quietFrom.Add(-params.Baseline)

	active := map[string]int{}  // minutes with events during the baseline
	recent := map[string]bool{} // any events since the quiet period began

	for key, v := range m.health {
		if v.Received <= 0 {
			continue
		}
		switch {
		case !key.bucket.Before(quietFrom):
			recent[key.tenantID] = true
		case !key.bucket.Before(baselineFrom):
			active[key.tenantID]++
		}
	}

	var out []string
	for tenantID, buckets := range active {
		if buckets >= params.MinActiveBuckets && !recent[tenantID] {
			out = append(out, tenantID)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *Memory) SyncSilenceEpisodes(_ context.Context, silent []string, now time.Time) (SilenceChange, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stillSilent := make(map[string]struct{}, len(silent))
	for _, id := range silent {
		stillSilent[id] = struct{}{}
	}

	var out SilenceChange
	for _, id := range silent {
		if _, open := m.silence[id]; open {
			continue // already alerted for this episode
		}
		since := m.silentSinceLocked(id, now.UTC())
		m.silence[id] = since
		out.Opened = append(out.Opened, SilenceEpisode{TenantID: id, SilentSince: since})
	}

	for id, since := range m.silence {
		if _, ok := stillSilent[id]; ok {
			continue
		}
		delete(m.silence, id)
		out.Recovered = append(out.Recovered, SilenceEpisode{TenantID: id, SilentSince: since})
	}

	// Map iteration order is random; a caller comparing results across stores
	// would otherwise see a difference that is not one.
	sortEpisodes(out.Opened)
	sortEpisodes(out.Recovered)
	return out, nil
}

// silentSinceLocked is the minute after their last event — when they actually
// stopped, not when the sweep noticed. Falls back to now when there is no
// history to date it from.
func (m *Memory) silentSinceLocked(tenantID string, now time.Time) time.Time {
	var last time.Time
	for key, v := range m.health {
		if key.tenantID != tenantID || v.Received <= 0 {
			continue
		}
		if key.bucket.After(last) {
			last = key.bucket
		}
	}
	if last.IsZero() {
		return now
	}
	return last.Add(time.Minute)
}

func sortEpisodes(eps []SilenceEpisode) {
	sort.Slice(eps, func(i, j int) bool { return eps[i].TenantID < eps[j].TenantID })
}

func (m *Memory) IngestActivity(_ context.Context, tenantID string, from, to time.Time) (IngestActivitySummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lo := from.UTC().Truncate(time.Minute)
	hi := to.UTC().Truncate(time.Minute)

	var s IngestActivitySummary
	for key, v := range m.health {
		if key.tenantID != tenantID {
			continue
		}
		if key.bucket.Before(lo) || key.bucket.After(hi) {
			continue
		}
		s.Received += v.Received
		if v.Received > 0 {
			s.ActiveBuckets++
		}
	}
	return s, nil
}
