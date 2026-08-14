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

// exposedTxn is a debit that left and never came back.
func exposedTxn(txnID, currency string, amount int64, ago time.Duration, backfill bool) *domain.Transaction {
	t := newExpiredTxn(tenantA, txnID, ago, time.Minute)
	t.Currency = currency
	t.AmountMinor = amount
	t.IsBackfill = backfill
	t.CustomerRefHash = "hash_" + txnID
	return t
}

func expose(t *testing.T, s store.Store, txns ...*domain.Transaction) {
	t.Helper()
	mustUpsert(t, s, txns...)
	if _, err := s.ClaimExpired(context.Background(), time.Now().UTC(), 1000); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
}

// ₦18.2M plus $4,000 is not a number. A single summed figure would be the most
// quotable wrong thing in the whole product.
func TestExposureNeverSumsAcrossCurrencies(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	now := time.Now().UTC()

	expose(t, s,
		exposedTxn("NGN-1", "NGN", 18_200_000_00, time.Hour, true),
		exposedTxn("USD-1", "USD", 400_000, time.Hour, true),
	)

	totals, bands, err := s.Exposure(context.Background(), tenantA, report.ScopeAll, now)
	if err != nil {
		t.Fatalf("Exposure: %v", err)
	}
	rep := report.ComputeExposure(tenantA, report.ScopeAll, totals, bands, now)

	if len(rep.Currencies) != 2 {
		t.Fatalf("currencies = %d, want 2 kept apart", len(rep.Currencies))
	}
	// Largest first, in each currency's own units.
	if rep.Currencies[0].Currency != "NGN" {
		t.Errorf("first = %s, want NGN", rep.Currencies[0].Currency)
	}
	for _, c := range rep.Currencies {
		if c.Transactions != 1 {
			t.Errorf("%s has %d transactions, want 1", c.Currency, c.Transactions)
		}
	}
	if rep.Notice == "" {
		t.Error("no notice saying what the number is")
	}
}

// The headline is "412 customers out of pocket, oldest 71 days".
func TestExposureCountsCustomersAndAge(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	now := time.Now().UTC()

	old := exposedTxn("OLD", "NGN", 1_000_000, 71*24*time.Hour, true)
	recent := exposedTxn("NEW", "NGN", 2_000_000, time.Hour, true)
	// Two transactions, one customer: a customer count that double-counts them
	// would overstate the blast radius.
	same := exposedTxn("SAME", "NGN", 500_000, 2*time.Hour, true)
	same.CustomerRefHash = recent.CustomerRefHash
	expose(t, s, old, recent, same)

	totals, bands, err := s.Exposure(context.Background(), tenantA, report.ScopeAll, now)
	if err != nil {
		t.Fatalf("Exposure: %v", err)
	}
	c := report.ComputeExposure(tenantA, report.ScopeAll, totals, bands, now).Currencies[0]

	if c.Transactions != 3 {
		t.Errorf("transactions = %d, want 3", c.Transactions)
	}
	if c.Customers != 2 {
		t.Errorf("customers = %d, want 2 — one customer had two transactions", c.Customers)
	}
	if c.AmountMinor != 3_500_000 {
		t.Errorf("amount = %d, want 3500000", c.AmountMinor)
	}
	if c.OldestAgeDays != 71 {
		t.Errorf("oldest = %d days, want 71", c.OldestAgeDays)
	}

	// Age bands, worst first, so a month-old debit is not buried under today's.
	if len(c.ByAge) == 0 || c.ByAge[0].Band != "over_30d" {
		t.Fatalf("age bands = %+v, want over_30d first", c.ByAge)
	}
	if c.ByAge[0].Transactions != 1 {
		t.Errorf("over_30d = %d transactions, want 1", c.ByAge[0].Transactions)
	}
}

// Money we cannot make a judgement about is not the same as money we know is
// gone, so it is reported beside the exposure rather than inside it.
func TestExposureSeparatesUnresolved(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	lost := exposedTxn("LOST", "NGN", 1_000_000, time.Hour, true)
	unknown := exposedTxn("UNKNOWN", "NGN", 4_000_000, time.Hour, true)
	mustUpsert(t, s, lost, unknown)
	// An ambiguous provider answer sends this one to suspect, not orphaned.
	if _, err := s.ApplyCredit(ctx, tenantA, "UNKNOWN", domain.StatusPendingUnknown, now); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	if _, err := s.ClaimExpired(ctx, now, 100); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	totals, bands, err := s.Exposure(ctx, tenantA, report.ScopeAll, now)
	if err != nil {
		t.Fatalf("Exposure: %v", err)
	}
	c := report.ComputeExposure(tenantA, report.ScopeAll, totals, bands, now).Currencies[0]

	if c.Unresolved.Transactions != 1 || c.Unresolved.AmountMinor != 4_000_000 {
		t.Errorf("unresolved = %+v, want 1 transaction of 4000000", c.Unresolved)
	}
	// It is still counted in the total, because the customer's money is still
	// out; the split says how much of that we can actually vouch for.
	if c.Transactions != 2 {
		t.Errorf("transactions = %d, want 2", c.Transactions)
	}
}

// Shadow mode is the replay: it must be separable from live traffic.
func TestExposureScopesReplayAndLiveApart(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	expose(t, s,
		exposedTxn("HIST-1", "NGN", 1_000_000, 48*time.Hour, true),
		exposedTxn("HIST-2", "NGN", 1_000_000, 48*time.Hour, true),
		exposedTxn("LIVE-1", "NGN", 5_000_000, time.Hour, false),
	)

	for _, tc := range []struct {
		scope report.Scope
		want  int
	}{
		{report.ScopeAll, 3},
		{report.ScopeBackfill, 2},
		{report.ScopeLive, 1},
	} {
		totals, bands, err := s.Exposure(ctx, tenantA, tc.scope, now)
		if err != nil {
			t.Fatalf("Exposure(%s): %v", tc.scope, err)
		}
		rep := report.ComputeExposure(tenantA, tc.scope, totals, bands, now)
		if len(rep.Currencies) == 0 {
			t.Fatalf("scope %s found nothing", tc.scope)
		}
		if got := rep.Currencies[0].Transactions; got != tc.want {
			t.Errorf("scope %s = %d transactions, want %d", tc.scope, got, tc.want)
		}
	}
}

// A settled transaction is not exposure, and neither is one already reversed.
func TestExposureExcludesResolvedMoney(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	settled := exposedTxn("SETTLED", "NGN", 9_000_000, time.Hour, true)
	mustUpsert(t, s, settled)
	if _, err := s.ApplyCredit(ctx, tenantA, "SETTLED", domain.StatusCompleted, now); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}

	totals, _, err := s.Exposure(ctx, tenantA, report.ScopeAll, now)
	if err != nil {
		t.Fatalf("Exposure: %v", err)
	}
	if len(totals) != 0 {
		t.Errorf("settled money was counted as exposure: %+v", totals)
	}
}

func TestIngestExposureReport(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	expose(t, f.store, exposedTxn("TX-1", "NGN", 18_200_000_00, 71*24*time.Hour, true))

	w := f.do(t, http.MethodGet, "/v1/reports/exposure?scope=backfill", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	if body["scope"] != "backfill" {
		t.Errorf("scope = %v", body["scope"])
	}
	currencies, _ := body["currencies"].([]any)
	if len(currencies) != 1 {
		t.Fatalf("currencies = %v", body["currencies"])
	}
	first, _ := currencies[0].(map[string]any)
	if first["oldest_age_days"] != float64(71) {
		t.Errorf("oldest_age_days = %v, want 71", first["oldest_age_days"])
	}

	// An unrecognised scope must not fall back to "all" and overstate exposure.
	if w := f.do(t, http.MethodGet, "/v1/reports/exposure?scope=everything", f.keyA, nil); w.Code != http.StatusBadRequest {
		t.Errorf("unknown scope = %d, want 400", w.Code)
	}
	// Tenant-scoped like everything else.
	w = f.do(t, http.MethodGet, "/v1/reports/exposure", f.keyB, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("tenant B = %d", w.Code)
	}
	if cs, _ := decodeBody(t, w)["currencies"].([]any); len(cs) != 0 {
		t.Errorf("tenant B saw tenant A's exposure: %v", cs)
	}
}

// --- store conformance ---

func testExposure(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	a := exposedTxn("EX-1", "NGN", 1_000_000, 40*24*time.Hour, true)
	b := exposedTxn("EX-2", "NGN", 2_000_000, time.Hour, false)
	b.CustomerRefHash = a.CustomerRefHash
	expose(t, s, a, b)

	totals, bands, err := s.Exposure(ctx, tenantA, report.ScopeAll, now)
	if err != nil {
		t.Fatalf("Exposure: %v", err)
	}
	if len(totals) != 1 {
		t.Fatalf("totals = %+v, want one currency", totals)
	}
	got := totals[0]
	if got.Transactions != 2 || got.AmountMinor != 3_000_000 {
		t.Errorf("total = %+v, want 2 transactions of 3000000", got)
	}
	if got.Customers != 1 {
		t.Errorf("customers = %d, want 1 — both transactions are the same customer", got.Customers)
	}
	if now.Sub(got.OldestDebitAt) < 39*24*time.Hour {
		t.Errorf("oldest = %v, want about 40 days ago", got.OldestDebitAt)
	}

	byBand := map[string]report.AgeBand{}
	for _, band := range bands {
		byBand[band.Band] = band
	}
	if byBand["over_30d"].Transactions != 1 || byBand["under_1d"].Transactions != 1 {
		t.Errorf("bands = %+v, want one over_30d and one under_1d", bands)
	}

	// Scope must filter in the database the same way it does in memory.
	live, _, err := s.Exposure(ctx, tenantA, report.ScopeLive, now)
	if err != nil {
		t.Fatalf("Exposure(live): %v", err)
	}
	if len(live) != 1 || live[0].Transactions != 1 {
		t.Errorf("live scope = %+v, want the one live transaction", live)
	}

	if other, _, err := s.Exposure(ctx, tenantB, report.ScopeAll, now); err != nil || len(other) != 0 {
		t.Errorf("tenant B saw %+v of tenant A's exposure (err=%v)", other, err)
	}
}

// Adding partial settlement made this report wrong until it was fixed: it summed
// the full debited amount, so a transaction where a fifth of the money arrived
// was reported as fully outstanding. Exposure is what left and has not arrived.
func testExposureCountsOnlyTheShortfall(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	txn := exposedTxn("EX-PART", "NGN", 5_000_000, time.Hour, false)
	mustUpsert(t, s, txn)

	// A fifth arrives, so four fifths are outstanding.
	credit := &domain.CreditEvent{
		TenantID: tenantA, TransactionID: "EX-PART", IdempotencyKey: "cp1",
		CreditAt: now, Status: domain.CreditSuccess, AmountMinor: 1_000_000,
	}
	if _, err := s.ApplyPartialCredit(ctx, tenantA, credit); err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}
	if _, err := s.ClaimExpired(ctx, now, 100); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	totals, bands, err := s.Exposure(ctx, tenantA, report.ScopeAll, now)
	if err != nil {
		t.Fatalf("Exposure: %v", err)
	}
	if len(totals) != 1 {
		t.Fatalf("totals = %+v", totals)
	}
	if totals[0].AmountMinor != 4_000_000 {
		t.Errorf("exposure = %d, want 4000000 — the fifth that arrived is not outstanding",
			totals[0].AmountMinor)
	}
	// The age breakdown has to agree with the total, or the report contradicts
	// itself between two of its own sections.
	var banded int64
	for _, b := range bands {
		banded += b.AmountMinor
	}
	if banded != totals[0].AmountMinor {
		t.Errorf("age bands sum to %d but the total says %d", banded, totals[0].AmountMinor)
	}
}
