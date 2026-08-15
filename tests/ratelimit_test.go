package tests

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/ratelimit"
)

func TestABucketRefillsOverTime(t *testing.T) {
	// Three a minute, two at once.
	l := ratelimit.New(3, time.Minute, 2)
	now := time.Now()

	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("acme", now); !ok {
			t.Fatalf("attempt %d refused inside the burst", i+1)
		}
	}

	ok, wait := l.Allow("acme", now)
	if ok {
		t.Fatal("a third call was allowed with a burst of two")
	}
	// Told how long to wait, and never zero — retry-after of nothing invites a
	// hot loop, which is the thing being prevented.
	if wait <= 0 {
		t.Errorf("Retry-After = %s, want something positive", wait)
	}

	// A minute later the bucket has refilled to its ceiling, not beyond it.
	later := now.Add(time.Minute)
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("acme", later); !ok {
			t.Fatalf("attempt %d after a refill was refused", i+1)
		}
	}
	if ok, _ := l.Allow("acme", later); ok {
		t.Error("the bucket refilled past its burst")
	}
}

// The whole point: one tenant exhausting its share must not touch anybody else.
func TestOneTenantCannotSpendAnothersBudget(t *testing.T) {
	l := ratelimit.New(1, time.Minute, 1)
	now := time.Now()

	if ok, _ := l.Allow("noisy", now); !ok {
		t.Fatal("the first call was refused")
	}
	if ok, _ := l.Allow("noisy", now); ok {
		t.Fatal("the noisy tenant was not limited")
	}

	if ok, _ := l.Allow("quiet", now); !ok {
		t.Error("a second tenant was refused because the first was noisy")
	}
}

// A rate of zero is how a deployment turns the limit off. Anything else would
// mean picking a number so large it looks like a limit and is not.
func TestAZeroRateAllowsEverything(t *testing.T) {
	l := ratelimit.New(0, time.Minute, 1)
	now := time.Now()

	for i := 0; i < 1000; i++ {
		if ok, _ := l.Allow("acme", now); !ok {
			t.Fatalf("call %d was refused with the limiter disabled", i+1)
		}
	}

	// And a nil limiter, so a caller that never configured one is not a panic.
	var none *ratelimit.Limiter
	if ok, _ := none.Allow("acme", now); !ok {
		t.Error("a nil limiter refused a call")
	}
}

// Reports scan a tenant's history, so one tenant looping over them is felt by
// every other tenant on the deployment.
func TestReportsAreRateLimitedPerTenant(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{reportsPerMinute: 2})

	var limited int
	for i := 0; i < 10; i++ {
		w := f.do(t, http.MethodGet, "/v1/reports/exposure?scope=all", f.keyA, nil)
		if w.Code == http.StatusTooManyRequests {
			limited++

			// The client is told how long to wait rather than left to guess.
			if after := w.Header().Get("Retry-After"); after == "" {
				t.Error("a 429 carried no Retry-After")
			} else if secs, err := strconv.Atoi(after); err != nil || secs <= 0 {
				t.Errorf("Retry-After = %q, want a positive number of seconds", after)
			}
		}
	}
	if limited == 0 {
		t.Fatal("ten reports in a row were all served; the limit did nothing")
	}

	// The other tenant is untouched, which is the entire point.
	if w := f.do(t, http.MethodGet, "/v1/reports/exposure?scope=all", f.keyB, nil); w.Code != http.StatusOK {
		t.Errorf("tenant B = %d, want 200 — one tenant's loop spent another's budget", w.Code)
	}
}

// Ingest is deliberately not rate limited. A debit that is refused is never
// observed, and a transaction never observed is one whose failure can never be
// detected — the customer believes they are covered and is not.
func TestIngestIsNotRateLimited(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{reportsPerMinute: 1})

	for i := 0; i < 50; i++ {
		body := debitBody("TX-" + strconv.Itoa(i))
		w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, body)
		if w.Code == http.StatusTooManyRequests {
			t.Fatalf("debit %d was rate limited; ReconSync would never see this transaction", i+1)
		}
	}
}
