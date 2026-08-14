package report

import (
	"fmt"
	"sort"
	"time"
)

// A window shorter than the rail's real settlement latency manufactures false
// orphans forever, and no amount of corroboration fixes it — it is a
// misconfiguration that looks exactly like a failing provider.
//
// Nothing measured this before, because nothing had the numbers. The provider
// scorecard does: it already computes settlement latency per rail from
// transactions that actually settled. This compares that against the window each
// rail is configured with, which is the whole of B6.

// SafetyMargin is how much headroom a window should have over observed latency.
//
// Set against p95 rather than the maximum: a window sized for the worst
// transaction ever seen would be so long that a genuine failure sits undetected
// for hours, which is the opposite failure and the one that costs a customer
// money. p95 with headroom means a small, known share of transactions get
// flagged late and re-settle, rather than a large unknown share being reversed
// wrongly.
const SafetyMargin = 1.5

// WindowFit is one rail's configured window judged against reality.
type WindowFit struct {
	Provider        string  `json:"provider"`
	WindowSeconds   int     `json:"window_seconds"`
	P95Seconds      float64 `json:"observed_p95_seconds"`
	MaxSeconds      float64 `json:"observed_max_seconds"`
	SettledSamples  int     `json:"settled_samples"`
	RecommendedSecs int     `json:"recommended_window_seconds,omitempty"`

	// Verdict is the sentence someone would act on.
	Verdict string `json:"verdict"`

	// TooTight says the window is at or below observed latency, which produces
	// orphans that were never failures.
	TooTight bool `json:"too_tight"`
}

// WindowReport is every rail, worst fit first.
type WindowReport struct {
	TenantID string      `json:"tenant_id"`
	From     time.Time   `json:"from"`
	To       time.Time   `json:"to"`
	Rails    []WindowFit `json:"rails"`
	Notice   string      `json:"notice"`
}

// WindowFor supplies the configured window for a rail.
type WindowFor func(provider string) (seconds int, ok bool)

// MinWindowSamples is how many settled transactions a rail needs before its
// latency is worth resizing a window against.
//
// The same reasoning as the scorecard's sample floor: three settlements do not
// describe a distribution, and a window resized on them would be resized again
// next week.
const MinWindowSamples = 30

// FitWindows judges each rail's configured window against what it actually does.
func FitWindows(tenantID string, from, to time.Time, stats []ProviderStat, window WindowFor) WindowReport {
	out := WindowReport{
		TenantID: tenantID,
		From:     from.UTC(),
		To:       to.UTC(),
		Rails:    []WindowFit{},
		Notice: "settlement latency is measured only on transactions that settled. " +
			"A window at or below it turns slow settlements into orphans that were never failures.",
	}

	for _, s := range stats {
		configured, ok := window(s.Provider)
		if !ok {
			// No rule matches this rail, so it runs on the default. Judging it
			// against a window nobody chose would be noise.
			continue
		}

		fit := WindowFit{
			Provider:       s.Provider,
			WindowSeconds:  configured,
			P95Seconds:     round1(s.P95),
			MaxSeconds:     round1(s.Max),
			SettledSamples: s.Settled,
		}

		switch {
		case s.Settled < MinWindowSamples:
			fit.Verdict = fmt.Sprintf("only %d settled transactions; too few to size a window on", s.Settled)
		case float64(configured) <= s.P95:
			// At or below p95 means at least one in twenty settlements is
			// already being called a failure.
			fit.TooTight = true
			fit.RecommendedSecs = recommend(s.P95)
			fit.Verdict = fmt.Sprintf(
				"window is %ds but 5%% of settlements take longer than %.0fs; "+
					"those are being detected as orphans. Widen to about %ds",
				configured, s.P95, fit.RecommendedSecs)
		case float64(configured) < s.P95*SafetyMargin:
			fit.RecommendedSecs = recommend(s.P95)
			fit.Verdict = fmt.Sprintf(
				"window is %ds against a p95 of %.0fs — little headroom. About %ds would be safer",
				configured, s.P95, fit.RecommendedSecs)
		default:
			fit.Verdict = "window comfortably clears observed latency"
		}
		out.Rails = append(out.Rails, fit)
	}

	// Worst first: a window that is actively manufacturing orphans leads.
	sort.SliceStable(out.Rails, func(i, j int) bool {
		a, b := out.Rails[i], out.Rails[j]
		if a.TooTight != b.TooTight {
			return a.TooTight
		}
		return a.headroom() < b.headroom()
	})
	return out
}

// headroom is how many times the p95 the window is. Rails with no observed
// latency sort last: no measurement is not a good result, it is no result.
func (f WindowFit) headroom() float64 {
	if f.P95Seconds <= 0 {
		return 1e9
	}
	return float64(f.WindowSeconds) / f.P95Seconds
}

// recommend rounds a suggested window up to something a human would type.
func recommend(p95 float64) int {
	want := int(p95*SafetyMargin) + 1
	for _, step := range []int{30, 60, 120, 300, 600, 900, 1800, 3600} {
		if want <= step {
			return step
		}
	}
	// Past an hour, round up to the next whole hour rather than inventing a
	// number nobody would choose.
	return ((want / 3600) + 1) * 3600
}
