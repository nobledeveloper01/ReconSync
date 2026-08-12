package tests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/report"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func stat(name string, settled, failed, suspect, open int) report.ProviderStat {
	return report.ProviderStat{
		Provider:  name,
		Total:     settled + failed + suspect + open,
		Settled:   settled,
		Failed:    failed,
		Suspect:   suspect,
		StillOpen: open,
	}
}

var scoreFrom, scoreTo = time.Now().Add(-30 * 24 * time.Hour), time.Now()

func TestScorecardRanksTheWorstRailFirst(t *testing.T) {
	card := report.ScoreProviders(tenantA, scoreFrom, scoreTo, []report.ProviderStat{
		stat("good", 990, 10, 0, 0),
		stat("awful", 800, 200, 0, 0),
		stat("middling", 950, 50, 0, 0),
	})

	if len(card.Providers) != 3 {
		t.Fatalf("providers = %d, want 3", len(card.Providers))
	}
	if card.Providers[0].Provider != "awful" || card.Providers[2].Provider != "good" {
		t.Errorf("order = %s, %s, %s", card.Providers[0].Provider,
			card.Providers[1].Provider, card.Providers[2].Provider)
	}
	if *card.Providers[0].FailureRate != 0.2 {
		t.Errorf("failure rate = %v, want 0.2", *card.Providers[0].FailureRate)
	}
	// The obvious misreading — that these are industry numbers — would put a
	// routing decision on evidence that does not exist.
	if card.Scope == "" {
		t.Error("scorecard does not say whose data it is")
	}
}

// One failure out of three is not a 33% failure rate, it is three transactions.
func TestScorecardFlagsThinSamples(t *testing.T) {
	card := report.ScoreProviders(tenantA, scoreFrom, scoreTo, []report.ProviderStat{
		stat("new-rail", 2, 1, 0, 0),
	})

	p := card.Providers[0]
	if !p.LowSample {
		t.Error("a three-transaction sample was presented as a rate to act on")
	}
	// And the ranking must not promote it: the order is what people read, so a
	// rail with one failure out of four must not sit above one failing 8% of a
	// hundred thousand.
	ranked := report.ScoreProviders(tenantA, scoreFrom, scoreTo, []report.ProviderStat{
		stat("thin", 3, 1, 0, 0),
		stat("real", 92000, 8000, 0, 0),
	})
	if ranked.Providers[0].Provider != "real" {
		t.Errorf("worst-first order = %s then %s; a four-transaction sample outranked a real failure rate",
			ranked.Providers[0].Provider, ranked.Providers[1].Provider)
	}
	// The rate is still reported — hiding it would be its own distortion.
	if p.FailureRate == nil {
		t.Error("no rate at all was reported")
	}
	if p.Verdict == "" {
		t.Error("no verdict")
	}
}

// A rail with nothing concluded is not good news, it is no news.
func TestScorecardWithNothingConcluded(t *testing.T) {
	card := report.ScoreProviders(tenantA, scoreFrom, scoreTo, []report.ProviderStat{
		stat("busy", 500, 0, 0, 0),
		stat("quiet", 0, 0, 0, 40),
	})

	if card.Providers[len(card.Providers)-1].Provider != "quiet" {
		t.Errorf("a rail with no verdict sorted above one with data: %+v", card.Providers)
	}
	for _, p := range card.Providers {
		if p.Provider == "quiet" && p.FailureRate != nil {
			t.Errorf("claimed a %v failure rate with nothing concluded", *p.FailureRate)
		}
	}
}

// Being unable to get an answer is its own reliability problem: every one of
// those cost a human an investigation.
func TestScorecardCountsUnresolvedAgainstTheRail(t *testing.T) {
	card := report.ScoreProviders(tenantA, scoreFrom, scoreTo, []report.ProviderStat{
		stat("ambiguous", 900, 0, 100, 0),
	})

	p := card.Providers[0]
	if p.FailureRate == nil || *p.FailureRate != 0.1 {
		t.Fatalf("failure rate = %v, want 0.1 — unresolved counts against the rail", p.FailureRate)
	}
	if p.Verdict == "settling normally" {
		t.Errorf("verdict = %q, want it to name the unresolved transactions", p.Verdict)
	}
}

// --- store conformance ---

func testProviderStats(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	// paystack settles one and loses one; flutterwave settles one.
	settled := newExpiredTxn(tenantA, "PS-OK", 10*time.Minute, time.Minute)
	lost := newExpiredTxn(tenantA, "PS-LOST", 10*time.Minute, time.Minute)
	other := newExpiredTxn(tenantA, "FW-OK", 10*time.Minute, time.Minute)
	other.Provider = "flutterwave"
	mustUpsert(t, s, settled, lost, other)

	for _, id := range []string{"PS-OK", "FW-OK"} {
		if _, err := s.ApplyCredit(ctx, tenantA, id, domain.StatusCompleted,
			now.Add(-9*time.Minute)); err != nil {
			t.Fatalf("ApplyCredit(%s): %v", id, err)
		}
	}
	if _, err := s.ClaimExpired(ctx, now, 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	stats, err := s.ProviderStats(ctx, tenantA, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("ProviderStats: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("stats = %+v, want two rails", stats)
	}

	byRail := map[string]report.ProviderStat{}
	for _, st := range stats {
		byRail[st.Provider] = st
	}
	ps := byRail["paystack"]
	if ps.Total != 2 || ps.Settled != 1 || ps.Failed != 1 {
		t.Errorf("paystack = %+v, want 2 total, 1 settled, 1 failed", ps)
	}
	// A settled transaction that took a minute to credit is a minute of latency.
	if ps.Max < 30 || ps.Max > 120 {
		t.Errorf("paystack max latency = %vs, want about 60s", ps.Max)
	}
	if fw := byRail["flutterwave"]; fw.Total != 1 || fw.Settled != 1 {
		t.Errorf("flutterwave = %+v, want 1 settled", fw)
	}

	// Another tenant's rails are not ours.
	if other, err := s.ProviderStats(ctx, tenantB, now.Add(-time.Hour), now.Add(time.Hour)); err != nil || len(other) != 0 {
		t.Errorf("tenant B saw %d of tenant A's rails (err=%v)", len(other), err)
	}
}

func TestIngestProviderScorecard(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()

	mustUpsert(t, f.store, newExpiredTxn(tenantA, "TX-LOST", 10*time.Minute, time.Minute))
	if _, err := f.store.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	w := f.do(t, http.MethodGet, "/v1/reports/providers", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	providers, _ := body["providers"].([]any)
	if len(providers) != 1 {
		t.Fatalf("providers = %v, want one rail", body["providers"])
	}
	first, _ := providers[0].(map[string]any)
	if first["provider"] != "paystack" {
		t.Errorf("provider = %v", first["provider"])
	}
	if first["failed"] != float64(1) {
		t.Errorf("failed = %v, want 1", first["failed"])
	}
	if body["scope"] == nil {
		t.Error("no scope stated")
	}

	// The period is validated the same way the compliance report validates it.
	for _, q := range []string{"?from=nonsense", "?from=2026-08-10&to=2026-08-01"} {
		if w := f.do(t, http.MethodGet, "/v1/reports/providers"+q, f.keyA, nil); w.Code != http.StatusBadRequest {
			t.Errorf("query %q = %d, want 400", q, w.Code)
		}
	}

	// And it is tenant-scoped like everything else.
	w = f.do(t, http.MethodGet, "/v1/reports/providers", f.keyB, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("tenant B status = %d", w.Code)
	}
	if provs, _ := decodeBody(t, w)["providers"].([]any); len(provs) != 0 {
		t.Errorf("tenant B saw tenant A's rails: %v", provs)
	}
}
