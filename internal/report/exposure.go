package report

import (
	"sort"
	"time"
)

// Scope selects which transactions an exposure report covers.
type Scope string

const (
	// ScopeAll is live traffic and replayed history together.
	ScopeAll Scope = "all"

	// ScopeBackfill is replayed history only — the shadow-mode question, "what
	// would you have found if we had been running for the last 90 days".
	ScopeBackfill Scope = "backfill"

	// ScopeLive excludes replayed history.
	ScopeLive Scope = "live"
)

// ValidScope reports whether s is one we know, so an unknown value is rejected
// rather than silently treated as "all" — which would overstate exposure.
func ValidScope(s Scope) bool {
	switch s {
	case ScopeAll, ScopeBackfill, ScopeLive:
		return true
	default:
		return false
	}
}

// ExposureTotal is one currency's outstanding position, as the store supplies it.
//
// Deliberately per currency. ₦18.2M plus $4,000 is not a number, and a single
// summed figure would be the most quotable wrong thing in the whole product.
type ExposureTotal struct {
	Currency      string
	Transactions  int
	Customers     int // distinct, by pseudonymised reference
	AmountMinor   int64
	OldestDebitAt time.Time

	// Unresolved is the subset we could not establish either way. It is money
	// that may be perfectly fine, so it is reported beside the exposure rather
	// than inside it.
	UnresolvedTransactions int
	UnresolvedAmountMinor  int64
}

// AgeBand is one currency's exposure in one age bracket.
type AgeBand struct {
	Currency     string
	Band         string
	Transactions int
	AmountMinor  int64
}

// AgeBands are the brackets, in order. Chosen so the top one is unambiguously a
// crisis: a customer out of pocket for over a month is not a backlog item.
var AgeBands = []struct {
	Name string
	From time.Duration // inclusive lower bound on age
}{
	{"over_30d", 30 * 24 * time.Hour},
	{"7d_to_30d", 7 * 24 * time.Hour},
	{"1d_to_7d", 24 * time.Hour},
	{"under_1d", 0},
}

// BandFor returns the bracket an age falls in.
func BandFor(age time.Duration) string {
	for _, b := range AgeBands {
		if age >= b.From {
			return b.Name
		}
	}
	return AgeBands[len(AgeBands)-1].Name
}

// CurrencyExposure is one currency, rendered.
type CurrencyExposure struct {
	Currency      string    `json:"currency"`
	Transactions  int       `json:"transactions"`
	Customers     int       `json:"customers_affected"`
	AmountMinor   int64     `json:"amount_minor"`
	OldestDebitAt time.Time `json:"oldest_debit_at"`
	OldestAgeDays int       `json:"oldest_age_days"`

	Unresolved UnresolvedExposure `json:"unresolved"`
	ByAge      []AgeBandView      `json:"by_age"`
}

// UnresolvedExposure is what we could not establish either way.
type UnresolvedExposure struct {
	Transactions int   `json:"transactions"`
	AmountMinor  int64 `json:"amount_minor"`
}

// AgeBandView is one bracket in the response.
type AgeBandView struct {
	Band         string `json:"band"`
	Transactions int    `json:"transactions"`
	AmountMinor  int64  `json:"amount_minor"`
}

// Exposure is the whole report.
type Exposure struct {
	TenantID string    `json:"tenant_id"`
	AsOf     time.Time `json:"as_of"`
	Scope    Scope     `json:"scope"`

	Currencies []CurrencyExposure `json:"currencies"`

	// Notice states what the number is and is not, because this report exists
	// to be quoted at people.
	Notice string `json:"notice"`
}

// ComputeExposure assembles the report, largest exposure first.
func ComputeExposure(tenantID string, scope Scope, totals []ExposureTotal, bands []AgeBand, now time.Time) Exposure {
	byCurrency := map[string][]AgeBandView{}
	for _, b := range bands {
		byCurrency[b.Currency] = append(byCurrency[b.Currency], AgeBandView{
			Band:         b.Band,
			Transactions: b.Transactions,
			AmountMinor:  b.AmountMinor,
		})
	}

	order := map[string]int{}
	for i, b := range AgeBands {
		order[b.Name] = i
	}

	out := Exposure{
		TenantID:   tenantID,
		AsOf:       now.UTC(),
		Scope:      scope,
		Currencies: []CurrencyExposure{},
		Notice: "debits with no confirmed credit and no confirmed reversal. " +
			"Amounts are per currency and are never summed across them. " +
			"ReconSync moves no money: these are transactions for your own system to settle or reverse.",
	}

	for _, t := range totals {
		c := CurrencyExposure{
			Currency:      t.Currency,
			Transactions:  t.Transactions,
			Customers:     t.Customers,
			AmountMinor:   t.AmountMinor,
			OldestDebitAt: t.OldestDebitAt.UTC(),
			Unresolved: UnresolvedExposure{
				Transactions: t.UnresolvedTransactions,
				AmountMinor:  t.UnresolvedAmountMinor,
			},
			ByAge: byCurrency[t.Currency],
		}
		if !t.OldestDebitAt.IsZero() {
			c.OldestAgeDays = int(now.Sub(t.OldestDebitAt).Hours() / 24)
		}
		if c.ByAge == nil {
			c.ByAge = []AgeBandView{}
		}
		sort.SliceStable(c.ByAge, func(i, j int) bool {
			return order[c.ByAge[i].Band] < order[c.ByAge[j].Band]
		})
		out.Currencies = append(out.Currencies, c)
	}

	// Largest exposure first, within a currency's own units — the only
	// comparison that means anything without an exchange rate we do not have.
	sort.SliceStable(out.Currencies, func(i, j int) bool {
		return out.Currencies[i].AmountMinor > out.Currencies[j].AmountMinor
	})
	return out
}
