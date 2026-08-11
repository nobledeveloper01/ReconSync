package tests

import (
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
)

func ruleTxn(txnType, provider, currency string, amount int64) *domain.Transaction {
	return &domain.Transaction{
		TransactionType: txnType,
		Provider:        provider,
		Currency:        currency,
		AmountMinor:     amount,
	}
}

func TestResolveFallsBackToDefault(t *testing.T) {
	got := rules.NewSet(nil).Resolve(ruleTxn("transfer", "paystack", "NGN", 1000))
	if got.Window != rules.DefaultWindow {
		t.Errorf("window = %s, want %s", got.Window, rules.DefaultWindow)
	}
	if got.Action != rules.ActionAutoReverse {
		t.Errorf("action = %s, want auto_reverse", got.Action)
	}
	if got.RuleID != 0 {
		t.Errorf("rule id = %d, want 0 for the default", got.RuleID)
	}
}

func TestResolveMatchesOnTransactionType(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 600, Action: rules.ActionAutoReverse, Enabled: true},
		{ID: 2, TransactionType: "bill_payment", WindowSeconds: 90, Action: rules.ActionAutoReverse, Enabled: true},
	})

	if got := set.Resolve(ruleTxn("transfer", "", "", 0)); got.Window != 600*time.Second {
		t.Errorf("transfer window = %s, want 10m", got.Window)
	}
	if got := set.Resolve(ruleTxn("bill_payment", "", "", 0)); got.Window != 90*time.Second {
		t.Errorf("bill_payment window = %s, want 90s", got.Window)
	}
	// An unmatched type falls back rather than borrowing another rule.
	if got := set.Resolve(ruleTxn("card", "", "", 0)); got.Window != rules.DefaultWindow {
		t.Errorf("card window = %s, want default", got.Window)
	}
}

func TestResolveDisabledRulesAreIgnored(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 30, Enabled: false},
	})
	if got := set.Resolve(ruleTxn("transfer", "", "", 0)); got.Window != rules.DefaultWindow {
		t.Errorf("window = %s, want default — a disabled rule was applied", got.Window)
	}
}

func TestResolveAmountBands(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, MinAmountMinor: rules.Amount(10_000_00), WindowSeconds: 900, Action: rules.ActionInvestigate, Enabled: true},
		{ID: 2, MaxAmountMinor: rules.Amount(100_00), WindowSeconds: 60, Action: rules.ActionAutoReverse, Enabled: true},
	})

	if got := set.Resolve(ruleTxn("", "", "", 50_00)); got.Window != 60*time.Second {
		t.Errorf("small amount window = %s, want 60s", got.Window)
	}
	if got := set.Resolve(ruleTxn("", "", "", 20_000_00)); got.Window != 900*time.Second {
		t.Errorf("large amount window = %s, want 900s", got.Window)
	}
	if got := set.Resolve(ruleTxn("", "", "", 5_000_00)); got.Window != rules.DefaultWindow {
		t.Errorf("mid amount window = %s, want default", got.Window)
	}
}

func TestResolveAmountBandsAreInclusive(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, MinAmountMinor: rules.Amount(1000), MaxAmountMinor: rules.Amount(2000), WindowSeconds: 45, Enabled: true},
	})
	for _, amount := range []int64{1000, 1500, 2000} {
		if got := set.Resolve(ruleTxn("", "", "", amount)); got.Window != 45*time.Second {
			t.Errorf("amount %d: window = %s, want 45s", amount, got.Window)
		}
	}
	for _, amount := range []int64{999, 2001} {
		if got := set.Resolve(ruleTxn("", "", "", amount)); got.Window != rules.DefaultWindow {
			t.Errorf("amount %d: window = %s, want default", amount, got.Window)
		}
	}
}

func TestResolveHighestPriorityWins(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 600, Priority: 0, Enabled: true},
		{ID: 2, TransactionType: "transfer", WindowSeconds: 120, Priority: 10, Enabled: true},
	})
	if got := set.Resolve(ruleTxn("transfer", "", "", 0)); got.RuleID != 2 {
		t.Errorf("rule = %d, want 2 (higher priority)", got.RuleID)
	}
}

func TestResolveMoreSpecificWinsAtEqualPriority(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 600, Enabled: true},
		{ID: 2, TransactionType: "transfer", Provider: "paystack", Currency: "NGN", WindowSeconds: 120, Enabled: true},
	})
	got := set.Resolve(ruleTxn("transfer", "paystack", "NGN", 0))
	if got.RuleID != 2 {
		t.Errorf("rule = %d, want 2 (more specific)", got.RuleID)
	}
	if got.Window != 120*time.Second {
		t.Errorf("window = %s, want 120s", got.Window)
	}
}

func TestResolvePriorityBeatsSpecificity(t *testing.T) {
	// An operator raising priority must be able to override a more specific
	// rule, or the priority column cannot force an outcome.
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 600, Priority: 5, Enabled: true},
		{ID: 2, TransactionType: "transfer", Provider: "paystack", Currency: "NGN", WindowSeconds: 120, Priority: 1, Enabled: true},
	})
	if got := set.Resolve(ruleTxn("transfer", "paystack", "NGN", 0)); got.RuleID != 1 {
		t.Errorf("rule = %d, want 1 (priority outranks specificity)", got.RuleID)
	}
}

func TestResolveIsDeterministicOnFullTie(t *testing.T) {
	// Same priority and specificity: the lower ID must win every time, so the
	// window cannot flip between scans.
	set := rules.NewSet([]rules.Rule{
		{ID: 7, TransactionType: "transfer", WindowSeconds: 600, Enabled: true},
		{ID: 3, TransactionType: "transfer", WindowSeconds: 120, Enabled: true},
	})
	for i := 0; i < 50; i++ {
		if got := set.Resolve(ruleTxn("transfer", "", "", 0)); got.RuleID != 3 {
			t.Fatalf("rule = %d, want 3 (lowest id on a full tie)", got.RuleID)
		}
	}
}

func TestResolveCarriesAction(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "card", WindowSeconds: 300, Action: rules.ActionAlertOnly, Enabled: true},
	})
	if got := set.Resolve(ruleTxn("card", "", "", 0)); got.Action != rules.ActionAlertOnly {
		t.Errorf("action = %s, want alert_only", got.Action)
	}
}
