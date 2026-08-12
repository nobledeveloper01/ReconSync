package report

import (
	"fmt"
	"sort"
	"time"
)

// MinScorecardSample is how many concluded transactions a provider needs before
// its failure rate is worth acting on.
//
// One failure out of three is not a 33% failure rate, it is three transactions.
// Reporting it as a rate invites someone to reroute their traffic on noise, so
// the rate is still shown but marked as a small sample.
const MinScorecardSample = 30

// ProviderStat is one rail's raw counts over a period, as the store supplies it.
type ProviderStat struct {
	Provider  string
	Total     int
	Settled   int
	Failed    int // reached orphaned or beyond: the rail did not deliver
	Suspect   int // we could not establish what happened
	StillOpen int

	// Settlement latency, in seconds, over settled transactions only. An
	// orphan never got a credit, so including it would be measuring nothing.
	P50, P95, Max float64
}

// ProviderScore is one rail, judged.
type ProviderScore struct {
	Provider  string `json:"provider"`
	Total     int    `json:"transactions"`
	Settled   int    `json:"settled"`
	Failed    int    `json:"failed"`
	Suspect   int    `json:"unresolved"`
	StillOpen int    `json:"still_open"`

	// FailureRate is over concluded transactions only. Nil when nothing has
	// concluded, because 0% and "no data" are different claims.
	FailureRate *float64 `json:"failure_rate,omitempty"`

	// LowSample says the rate is real but too thin to route on.
	LowSample bool `json:"low_sample,omitempty"`

	Settlement Latency `json:"settlement_latency"`

	// Verdict is the sentence someone would actually repeat in a meeting.
	Verdict string `json:"verdict"`
}

// Scorecard ranks a tenant's rails.
type Scorecard struct {
	TenantID string    `json:"tenant_id"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`

	Providers []ProviderScore `json:"providers"`

	// Scope states whose data this is, because the obvious misreading — that
	// these are industry numbers — would make a routing decision on evidence
	// that does not exist. ReconSync is self-hosted: we see one customer's
	// traffic, never the market's.
	Scope string `json:"scope"`
}

// ScoreProviders builds the scorecard, worst first.
func ScoreProviders(tenantID string, from, to time.Time, stats []ProviderStat) Scorecard {
	card := Scorecard{
		TenantID:  tenantID,
		From:      from.UTC(),
		To:        to.UTC(),
		Providers: []ProviderScore{},
		Scope:     "this deployment's own traffic only; not an industry benchmark",
	}

	for _, s := range stats {
		score := ProviderScore{
			Provider:  s.Provider,
			Total:     s.Total,
			Settled:   s.Settled,
			Failed:    s.Failed,
			Suspect:   s.Suspect,
			StillOpen: s.StillOpen,
			Settlement: Latency{
				Samples: s.Settled,
				P50:     round1(s.P50),
				P95:     round1(s.P95),
				Max:     round1(s.Max),
			},
		}
		if s.Settled == 0 {
			score.Settlement = Latency{}
		}

		// Suspect counts as concluded: we finished with it, we just could not
		// say which way. Leaving it out would flatter a rail whose answers we
		// cannot get, which is itself a reliability problem.
		concluded := s.Settled + s.Failed + s.Suspect
		if concluded > 0 {
			rate := float64(s.Failed+s.Suspect) / float64(concluded)
			score.FailureRate = &rate
			score.LowSample = concluded < MinScorecardSample
		}
		score.Verdict = verdictFor(score, concluded)
		card.Providers = append(card.Providers, score)
	}

	// Worst first, in three tiers: rails with a rate worth acting on, then rails
	// whose rate is too thin to act on, then rails with no rate at all.
	//
	// The tiers matter because the ranking is what people read. A rail with one
	// failure out of four would otherwise sit at the top of the "worst" list
	// above a rail failing 8% of a hundred thousand — technically a higher
	// percentage, and entirely the wrong thing to look at first.
	sort.SliceStable(card.Providers, func(i, j int) bool {
		a, b := card.Providers[i], card.Providers[j]
		if ra, rb := rank(a), rank(b); ra != rb {
			return ra < rb
		}
		if a.FailureRate != nil && b.FailureRate != nil && *a.FailureRate != *b.FailureRate {
			return *a.FailureRate > *b.FailureRate
		}
		return a.Settlement.P95 > b.Settlement.P95
	})
	return card
}

// rank puts a provider in its tier: 0 actionable, 1 too thin to act on, 2 no
// verdict at all.
func rank(s ProviderScore) int {
	switch {
	case s.FailureRate == nil:
		return 2
	case s.LowSample:
		return 1
	default:
		return 0
	}
}

func verdictFor(s ProviderScore, concluded int) string {
	switch {
	case concluded == 0:
		return "nothing has concluded in this period; no judgement to make"
	case s.LowSample:
		return fmt.Sprintf("only %d concluded transactions; too thin to route on", concluded)
	case *s.FailureRate >= 0.05:
		return fmt.Sprintf("%.1f%% of transactions did not settle; worth raising with them",
			*s.FailureRate*100)
	case s.Suspect > 0 && s.Suspect*20 >= concluded:
		// Being unable to get an answer is its own failure: every one of these
		// cost a human an investigation.
		return fmt.Sprintf("%d transactions could not be resolved either way; their status API is the problem, not their settlement", s.Suspect)
	default:
		return "settling normally"
	}
}
