package tests

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/report"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
)

func latencyStat(name string, settled int, p95, max float64) report.ProviderStat {
	return report.ProviderStat{
		Provider: name, Total: settled, Settled: settled, P95: p95, Max: max,
	}
}

func fixedWindow(seconds int) report.WindowFor {
	return func(string) (int, bool) { return seconds, true }
}

// The failure this exists to catch: a window at or below the rail's real
// latency means at least one settlement in twenty is already being called a
// failure.
func TestWindowFitFlagsAWindowShorterThanReality(t *testing.T) {
	// p95 of 280s against a 250s window: 5% of settlements already exceed it.
	rep := report.FitWindows(tenantA, time.Now().Add(-24*time.Hour), time.Now(),
		[]report.ProviderStat{latencyStat("paystack", 500, 280, 340)}, fixedWindow(250))

	if len(rep.Rails) != 1 {
		t.Fatalf("rails = %+v", rep.Rails)
	}
	fit := rep.Rails[0]
	if !fit.TooTight {
		t.Fatalf("a 300s window against a 280s p95 was not flagged: %+v", fit)
	}
	// The recommendation has to clear p95 with headroom, or it just moves the
	// problem a little further out.
	if float64(fit.RecommendedSecs) < fit.P95Seconds*1.4 {
		t.Errorf("recommended %ds against a p95 of %.0fs — not enough headroom",
			fit.RecommendedSecs, fit.P95Seconds)
	}
	if !strings.Contains(fit.Verdict, "orphans") {
		t.Errorf("verdict = %q, want it to name the consequence", fit.Verdict)
	}
}

// A window with room to spare is fine and must not be nagged about.
func TestWindowFitAcceptsAComfortableWindow(t *testing.T) {
	rep := report.FitWindows(tenantA, time.Now().Add(-24*time.Hour), time.Now(),
		[]report.ProviderStat{latencyStat("flutterwave", 500, 12, 40)}, fixedWindow(300))

	fit := rep.Rails[0]
	if fit.TooTight {
		t.Errorf("a 300s window against a 12s p95 was flagged: %+v", fit)
	}
	if fit.RecommendedSecs != 0 {
		t.Errorf("recommended a change to a comfortable window: %+v", fit)
	}
	if !strings.Contains(fit.Verdict, "comfortably") {
		t.Errorf("verdict = %q", fit.Verdict)
	}
}

// Three settlements do not describe a distribution, and a window resized on
// them would be resized again next week.
func TestWindowFitWillNotSizeOnAThinSample(t *testing.T) {
	rep := report.FitWindows(tenantA, time.Now().Add(-24*time.Hour), time.Now(),
		[]report.ProviderStat{latencyStat("newrail", 3, 280, 340)}, fixedWindow(300))

	fit := rep.Rails[0]
	if fit.TooTight {
		t.Error("flagged a window on three settlements")
	}
	if fit.RecommendedSecs != 0 {
		t.Error("recommended a window from three settlements")
	}
	if !strings.Contains(fit.Verdict, "too few") {
		t.Errorf("verdict = %q, want it to say the sample is too thin", fit.Verdict)
	}
}

// Little headroom is worth saying before it becomes a problem, without calling
// it broken.
func TestWindowFitWarnsOnThinHeadroom(t *testing.T) {
	// p95 200s, window 250s: above p95, but under the 1.5x margin.
	rep := report.FitWindows(tenantA, time.Now().Add(-24*time.Hour), time.Now(),
		[]report.ProviderStat{latencyStat("paystack", 500, 200, 260)}, fixedWindow(250))

	fit := rep.Rails[0]
	if fit.TooTight {
		t.Error("a window above p95 was called too tight")
	}
	if fit.RecommendedSecs == 0 {
		t.Error("no recommendation despite thin headroom")
	}
	if !strings.Contains(fit.Verdict, "headroom") {
		t.Errorf("verdict = %q", fit.Verdict)
	}
}

// Worst first: a window actively manufacturing orphans leads the list.
func TestWindowFitRanksTheWorstFirst(t *testing.T) {
	rep := report.FitWindows(tenantA, time.Now().Add(-24*time.Hour), time.Now(),
		[]report.ProviderStat{
			latencyStat("fine", 500, 10, 20),
			latencyStat("broken", 500, 280, 340),
			latencyStat("tight", 500, 200, 260),
		},
		func(rail string) (int, bool) {
			switch rail {
			case "broken":
				return 300, true
			case "tight":
				return 250, true
			default:
				return 300, true
			}
		})

	if rep.Rails[0].Provider != "broken" {
		t.Errorf("order = %s, %s, %s; want broken first",
			rep.Rails[0].Provider, rep.Rails[1].Provider, rep.Rails[2].Provider)
	}
	if rep.Rails[2].Provider != "fine" {
		t.Errorf("the comfortable rail did not sort last: %+v", rep.Rails)
	}
}

// The recommendation must be a number a human would actually type.
func TestWindowFitRecommendsRoundNumbers(t *testing.T) {
	for _, p95 := range []float64{7, 40, 130, 280, 700} {
		rep := report.FitWindows(tenantA, time.Now().Add(-time.Hour), time.Now(),
			[]report.ProviderStat{latencyStat("rail", 500, p95, p95*1.2)}, fixedWindow(1))
		got := rep.Rails[0].RecommendedSecs

		round := map[int]bool{30: true, 60: true, 120: true, 300: true, 600: true, 900: true, 1800: true, 3600: true}
		if !round[got] && got%3600 != 0 {
			t.Errorf("p95 %.0fs recommended %ds, which is not a number anyone would choose", p95, got)
		}
		if float64(got) <= p95 {
			t.Errorf("p95 %.0fs recommended %ds, which is not even above it", p95, got)
		}
	}
}

func TestIngestWindowFitReport(t *testing.T) {
	// A tenant whose transfer window is 300s, against a rail that takes longer.
	set := rules.NewSet([]rules.Rule{{
		ID: 1, TransactionType: "transfer", WindowSeconds: 300,
		Action: rules.ActionAutoReverse, Enabled: true, Priority: 10,
	}})
	f := newIngestFixture(t, fixtureOpts{ruleSet: set})

	// Settled transactions that each took four minutes, well past the window.
	for i := 0; i < 40; i++ {
		txn := newExpiredTxn(tenantA, "TX-"+string(rune('a'+i%26))+string(rune('a'+i/26)), 30*time.Minute, time.Minute)
		mustUpsert(t, f.store, txn)
		// Six minutes to settle, against a five-minute window.
		credited := txn.DebitAt.Add(6 * time.Minute)
		if _, err := f.store.ApplyCredit(t.Context(), tenantA, txn.TransactionID,
			"completed", credited); err != nil {
			t.Fatalf("ApplyCredit: %v", err)
		}
	}

	w := f.do(t, http.MethodGet, "/v1/reports/window-fit", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	railsAny, _ := body["rails"].([]any)
	if len(railsAny) != 1 {
		t.Fatalf("rails = %v", body["rails"])
	}
	rail, _ := railsAny[0].(map[string]any)
	if rail["too_tight"] != true {
		t.Errorf("a 300s window against 360s settlements was not flagged: %v", rail)
	}
	if body["notice"] == nil {
		t.Error("no notice explaining what the number means")
	}
}
