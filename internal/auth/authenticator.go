package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ErrUnauthenticated covers every authentication failure. It is deliberately
// single: callers must not be able to tell an unknown key from a revoked one
// from a wrong one.
var ErrUnauthenticated = errors.New("auth: unauthenticated")

// Record is the stored form of an API key.
type Record struct {
	ID          string
	TenantID    string
	Prefix      string
	Hash        string
	Scopes      []string
	Environment Environment
	RevokedAt   *time.Time
}

// Lookup resolves a key prefix to its record. It is not tenant-scoped because
// it is what determines the tenant.
type Lookup interface {
	APIKeyByPrefix(ctx context.Context, prefix string) (*Record, error)
	TouchAPIKey(ctx context.Context, id string) error
}

// Principal is the authenticated caller.
type Principal struct {
	TenantID    string
	KeyID       string
	Environment Environment
	Scopes      []string
}

// Scopes an API key can hold. A key with none has full access, which is what a
// first-run key gets and what every key issued before scopes existed has.
//
// The distinction that matters: an ingest key lives in the customer's
// transaction service, where it is handled by the most code and leaks most
// easily. Registering a webhook endpoint decides where reversal payloads are
// sent, and must not be something that key can do.
const (
	// ScopeEventsWrite reports transactions. The high-volume, low-privilege key.
	ScopeEventsWrite = "events:write"

	// ScopeReportsRead reads reports and transaction state.
	ScopeReportsRead = "reports:read"

	// ScopeEndpointsWrite changes where webhooks are delivered.
	ScopeEndpointsWrite = "endpoints:write"
)

// HasScope reports whether the principal holds a scope. An empty scope list on
// the key means full access, which is what a first-run key gets.
func (p Principal) HasScope(want string) bool {
	if len(p.Scopes) == 0 {
		return true
	}
	for _, s := range p.Scopes {
		if s == want {
			return true
		}
	}
	return false
}

type ctxKey struct{}

// WithPrincipal stores a principal on a context.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// PrincipalFrom returns the authenticated caller, if any.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

const (
	// DefaultCacheTTL bounds how long a verified key stays trusted, and so how
	// long a revocation takes to take effect.
	DefaultCacheTTL = 60 * time.Second

	// maxCacheEntries caps memory. Reaching it means many distinct keys are in
	// play, so the whole cache is dropped rather than evicted one by one.
	maxCacheEntries = 4096
)

// Authenticator verifies bearer keys.
//
// argon2id is deliberately expensive — around 64 MiB per verification — which is
// correct for a credential and ruinous on a path taking thousands of events a
// second. Successful verifications are therefore cached against a SHA-256 of the
// presented secret, so the expensive path runs about once per key per TTL. The
// cache holds no plaintext and a miss simply costs a full verification.
type Authenticator struct {
	lookup Lookup
	ttl    time.Duration
	now    func() time.Time

	mu    sync.RWMutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	principal Principal
	expiresAt time.Time
}

// Options configures an Authenticator.
type Options struct {
	CacheTTL time.Duration
	Now      func() time.Time
}

// New builds an Authenticator.
func New(l Lookup, opts Options) (*Authenticator, error) {
	if l == nil {
		return nil, errors.New("auth: lookup is required")
	}
	ttl := opts.CacheTTL
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Authenticator{
		lookup: l,
		ttl:    ttl,
		now:    now,
		cache:  make(map[string]cacheEntry),
	}, nil
}

// Authenticate verifies a presented key and returns the caller it belongs to.
func (a *Authenticator) Authenticate(ctx context.Context, secret string) (Principal, error) {
	if secret == "" {
		return Principal{}, ErrUnauthenticated
	}

	sum := sha256.Sum256([]byte(secret))
	cacheKey := hex.EncodeToString(sum[:])

	if p, ok := a.fromCache(cacheKey); ok {
		return p, nil
	}

	rec, err := a.lookup.APIKeyByPrefix(ctx, Prefix(secret))
	if err != nil || rec == nil {
		// Includes not-found. Never distinguish it from a bad secret.
		return Principal{}, ErrUnauthenticated
	}
	if rec.RevokedAt != nil {
		return Principal{}, ErrUnauthenticated
	}

	ok, err := Verify(rec.Hash, secret)
	if err != nil || !ok {
		return Principal{}, ErrUnauthenticated
	}

	p := Principal{
		TenantID:    rec.TenantID,
		KeyID:       rec.ID,
		Environment: rec.Environment,
		Scopes:      rec.Scopes,
	}
	a.store(cacheKey, p)

	// Best effort: roughly once per TTL per key, not once per request.
	_ = a.lookup.TouchAPIKey(ctx, rec.ID)

	return p, nil
}

func (a *Authenticator) fromCache(key string) (Principal, bool) {
	a.mu.RLock()
	entry, ok := a.cache[key]
	a.mu.RUnlock()

	if !ok || a.now().After(entry.expiresAt) {
		return Principal{}, false
	}
	return entry.principal, true
}

func (a *Authenticator) store(key string, p Principal) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.cache) >= maxCacheEntries {
		a.cache = make(map[string]cacheEntry, maxCacheEntries)
	}
	a.cache[key] = cacheEntry{principal: p, expiresAt: a.now().Add(a.ttl)}
}

// Invalidate drops a key from the cache so a revocation takes effect at once.
func (a *Authenticator) Invalidate(secret string) {
	sum := sha256.Sum256([]byte(secret))
	key := hex.EncodeToString(sum[:])

	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.cache, key)
}

// BearerToken extracts a bearer credential from a request.
func BearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
