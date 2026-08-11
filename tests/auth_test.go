package tests

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
)

// countingLookup records how often the expensive path is taken, so the cache
// can be asserted rather than assumed.
type countingLookup struct {
	mu      sync.Mutex
	rec     *auth.Record
	err     error
	lookups int
	touches int
}

func (c *countingLookup) APIKeyByPrefix(_ context.Context, _ string) (*auth.Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookups++
	if c.err != nil {
		return nil, c.err
	}
	return c.rec, nil
}

func (c *countingLookup) TouchAPIKey(_ context.Context, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touches++
	return nil
}

func (c *countingLookup) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lookups, c.touches
}

func TestGenerateProducesUsableKey(t *testing.T) {
	for _, env := range []auth.Environment{auth.EnvTest, auth.EnvLive} {
		key, err := auth.Generate(env)
		if err != nil {
			t.Fatalf("Generate(%s): %v", env, err)
		}

		if !strings.HasPrefix(key.Secret, "rs_"+string(env)+"_") {
			t.Errorf("secret %q lacks the rs_%s_ prefix", key.Secret, env)
		}
		if key.Prefix != key.Secret[:auth.PrefixLen] {
			t.Errorf("prefix %q is not the leading %d characters of the secret", key.Prefix, auth.PrefixLen)
		}
		// The stored hash must never contain the secret itself.
		if strings.Contains(key.Hash, key.Secret) {
			t.Error("stored hash contains the plaintext secret")
		}

		ok, err := auth.Verify(key.Hash, key.Secret)
		if err != nil || !ok {
			t.Errorf("generated key does not verify against its own hash: ok=%v err=%v", ok, err)
		}
	}
}

func TestGenerateRejectsUnknownEnvironment(t *testing.T) {
	if _, err := auth.Generate("staging"); err == nil {
		t.Error("accepted an unknown environment")
	}
}

func TestKeysAreUnique(t *testing.T) {
	a, err := auth.Generate(auth.EnvLive)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	b, err := auth.Generate(auth.EnvLive)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if a.Secret == b.Secret {
		t.Fatal("two generated keys are identical")
	}
	// A key must not verify against another key's hash.
	if ok, _ := auth.Verify(a.Hash, b.Secret); ok {
		t.Error("a key verified against a different key's hash")
	}
}

func TestVerifyRejectsWrongSecretAndMalformedHash(t *testing.T) {
	key, err := auth.Generate(auth.EnvTest)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if ok, _ := auth.Verify(key.Hash, key.Secret+"x"); ok {
		t.Error("verified a secret with an extra character")
	}
	if ok, _ := auth.Verify(key.Hash, ""); ok {
		t.Error("verified an empty secret")
	}

	for _, bad := range []string{
		"",
		"not-a-hash",
		"$argon2i$v=19$m=65536,t=1,p=4$c2FsdA$aGFzaA", // wrong algorithm
		"$argon2id$v=19$m=65536,t=1,p=4$!!!$aGFzaA",   // undecodable salt
	} {
		if _, err := auth.Verify(bad, key.Secret); err == nil {
			t.Errorf("malformed hash %q did not error", bad)
		}
	}
}

func TestPrefixAndEnvironmentOf(t *testing.T) {
	// A short or malformed token must produce a miss, never a panic.
	if got := auth.Prefix("rs_"); got != "rs_" {
		t.Errorf("Prefix of a short token = %q, want it returned whole", got)
	}

	env, ok := auth.EnvironmentOf("rs_live_abc")
	if !ok || env != auth.EnvLive {
		t.Errorf("EnvironmentOf = %v/%v, want live/true", env, ok)
	}
	for _, bad := range []string{"", "rs_", "xx_live_abc", "rs_staging_abc"} {
		if _, ok := auth.EnvironmentOf(bad); ok {
			t.Errorf("EnvironmentOf(%q) reported a valid environment", bad)
		}
	}
}

func newAuthenticator(t *testing.T, ttl time.Duration) (*auth.Authenticator, *countingLookup, auth.Key) {
	t.Helper()

	key, err := auth.Generate(auth.EnvLive)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	lookup := &countingLookup{rec: &auth.Record{
		ID:          "key_1",
		TenantID:    tenantA,
		Prefix:      key.Prefix,
		Hash:        key.Hash,
		Environment: auth.EnvLive,
	}}

	a, err := auth.New(lookup, auth.Options{CacheTTL: ttl})
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	return a, lookup, key
}

func TestAuthenticateResolvesTenant(t *testing.T) {
	a, _, key := newAuthenticator(t, time.Minute)

	p, err := a.Authenticate(context.Background(), key.Secret)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if p.TenantID != tenantA {
		t.Errorf("tenant = %q, want %q", p.TenantID, tenantA)
	}
	if p.Environment != auth.EnvLive {
		t.Errorf("environment = %q, want live", p.Environment)
	}
}

func TestAuthenticateRejectsBadCredentials(t *testing.T) {
	a, lookup, key := newAuthenticator(t, time.Minute)
	ctx := context.Background()

	// Every failure must surface as the same error — a caller must not be able
	// to tell an unknown key from a wrong one.
	if _, err := a.Authenticate(ctx, ""); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("empty secret: %v, want ErrUnauthenticated", err)
	}
	if _, err := a.Authenticate(ctx, key.Secret+"tampered"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("tampered secret: %v, want ErrUnauthenticated", err)
	}

	lookup.mu.Lock()
	lookup.err = errors.New("not found")
	lookup.mu.Unlock()
	if _, err := a.Authenticate(ctx, key.Secret); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("unknown prefix: %v, want ErrUnauthenticated", err)
	}
}

func TestAuthenticateRejectsRevokedKey(t *testing.T) {
	a, lookup, key := newAuthenticator(t, time.Minute)

	revoked := time.Now().UTC()
	lookup.mu.Lock()
	lookup.rec.RevokedAt = &revoked
	lookup.mu.Unlock()

	if _, err := a.Authenticate(context.Background(), key.Secret); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("revoked key authenticated: %v", err)
	}
}

// argon2id costs ~64 MiB per verification, so the cache is what keeps auth off
// the ingest hot path. If it stops working, throughput collapses silently.
func TestAuthenticateCachesVerifications(t *testing.T) {
	a, lookup, key := newAuthenticator(t, time.Minute)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if _, err := a.Authenticate(ctx, key.Secret); err != nil {
			t.Fatalf("Authenticate %d: %v", i, err)
		}
	}

	lookups, touches := lookup.counts()
	if lookups != 1 {
		t.Errorf("%d lookups for 20 authentications, want 1", lookups)
	}
	if touches != 1 {
		t.Errorf("%d last-used writes for 20 authentications, want 1", touches)
	}
}

func TestAuthenticateCacheExpires(t *testing.T) {
	a, lookup, key := newAuthenticator(t, 20*time.Millisecond)
	ctx := context.Background()

	if _, err := a.Authenticate(ctx, key.Secret); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := a.Authenticate(ctx, key.Secret); err != nil {
		t.Fatalf("Authenticate after expiry: %v", err)
	}

	if lookups, _ := lookup.counts(); lookups != 2 {
		t.Errorf("%d lookups, want 2 — the cache outlived its TTL", lookups)
	}
}

func TestInvalidateDropsCachedKey(t *testing.T) {
	a, lookup, key := newAuthenticator(t, time.Hour)
	ctx := context.Background()

	if _, err := a.Authenticate(ctx, key.Secret); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	a.Invalidate(key.Secret)

	// Revocation must take effect immediately once the cache entry is dropped.
	revoked := time.Now().UTC()
	lookup.mu.Lock()
	lookup.rec.RevokedAt = &revoked
	lookup.mu.Unlock()

	if _, err := a.Authenticate(ctx, key.Secret); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("revoked key still authenticated after Invalidate: %v", err)
	}
}

func TestAuthenticateIsConcurrencySafe(t *testing.T) {
	a, _, key := newAuthenticator(t, time.Minute)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := a.Authenticate(ctx, key.Secret); err != nil {
				t.Errorf("Authenticate: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestAuthNewRequiresLookup(t *testing.T) {
	if _, err := auth.New(nil, auth.Options{}); err == nil {
		t.Error("accepted a nil lookup")
	}
}

func TestPrincipalScopes(t *testing.T) {
	// An empty scope list means full access, which is what a first-run key gets.
	if !(auth.Principal{}).HasScope("events:write") {
		t.Error("empty scope list should grant access")
	}
	p := auth.Principal{Scopes: []string{"events:write"}}
	if !p.HasScope("events:write") {
		t.Error("granted scope not recognised")
	}
	if p.HasScope("keys:admin") {
		t.Error("ungranted scope was allowed")
	}
}

func TestPrincipalContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	if _, ok := auth.PrincipalFrom(ctx); ok {
		t.Error("bare context reported a principal")
	}

	want := auth.Principal{TenantID: tenantA, KeyID: "key_1"}
	got, ok := auth.PrincipalFrom(auth.WithPrincipal(ctx, want))
	if !ok || got.TenantID != want.TenantID || got.KeyID != want.KeyID {
		t.Errorf("round trip = %+v/%v, want %+v", got, ok, want)
	}
}

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"Bearer rs_live_abc":  "rs_live_abc",
		"bearer rs_live_abc":  "rs_live_abc", // scheme is case-insensitive
		"Bearer  rs_live_abc": "rs_live_abc",
		"":                    "",
		"rs_live_abc":         "",
		"Basic rs_live_abc":   "",
		"Bearer":              "",
		"Bearer ":             "",
	}
	for header, want := range cases {
		r, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		if got := auth.BearerToken(r); got != want {
			t.Errorf("BearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}
