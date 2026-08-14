package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/internal/licence"
	"github.com/nobledeveloper01/ReconSync/internal/migrate"
	"github.com/nobledeveloper01/ReconSync/internal/provider"
)

// The preflight, rewritten around the failure modes that are silent.
//
// Every check below exists because of a way this system can stop protecting a
// customer without stopping, or erroring, or looking any different: a schema
// the binary does not expect, a chain nobody can prove was not rewritten, a
// tenant whose reversals have nowhere to go. A deployment can be perfectly
// healthy by every ordinary measure and be none of those things.
//
// Failures and warnings are kept apart on purpose. A failure means the
// deployment is broken now; a warning means it is running with a guarantee
// switched off. Reporting the second as the first trains people to ignore both.

type check struct {
	name   string
	detail string
	fatal  bool
}

func doctor(ctx context.Context) error {
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	var problems, warnings []check

	ok := func(name, detail string) { fmt.Printf("✓ %s%s\n", name, suffix(detail)) }
	fail := func(name, detail string) {
		problems = append(problems, check{name, detail, true})
		fmt.Printf("✗ %s — %s\n", name, detail)
	}
	warn := func(name, detail string) {
		warnings = append(warnings, check{name, detail, false})
		fmt.Printf("! %s — %s\n", name, detail)
	}

	// --- the database ---
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database unreachable: %w", err)
	}
	ok("database reachable", "")

	// Pending migrations mean the binary is running against a schema it does
	// not expect, which surfaces later as a missing column mid-transaction.
	schemaReady := false
	applied, pending, err := migrate.Status(ctx, pool)
	switch {
	case err != nil:
		fail("schema", "could not read the migration ledger: "+err.Error())
	case len(pending) > 0:
		fail("schema", fmt.Sprintf("%d migrations pending (%s) — run: reconsyncctl migrate up",
			len(pending), pending[0]))
	default:
		schemaReady = true
		ok("schema up to date", fmt.Sprintf("%d migrations", len(applied)))
	}

	// Clock skew breaks webhook signature verification with an error that tells
	// the operator nothing.
	var dbNow time.Time
	if err := pool.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
		fail("clock", "could not read the database time: "+err.Error())
	} else {
		skew := time.Since(dbNow)
		if skew < 0 {
			skew = -skew
		}
		if skew > 5*time.Second {
			fail("clock", fmt.Sprintf("skew of %s against the database — webhook signatures "+
				"and reconciliation windows will misbehave", skew.Round(time.Millisecond)))
		} else {
			ok("clock skew", skew.Round(time.Millisecond).String())
		}
	}

	// --- the guarantees that can be silently absent ---
	checkSecrets(ok, fail)
	checkLicence(ok, fail, warn)
	checkCheckpoints(ok, warn)
	checkProviders(ok, fail, warn)
	// Skipped when the schema is not there: every one of these would fail for
	// the same reason, and a cascade of errors buries the one that matters.
	if schemaReady {
		checkDeliveryTargets(ctx, pool, ok, warn)
	}

	fmt.Println()
	switch {
	case len(problems) > 0:
		return fmt.Errorf("%d checks failed, %d warnings", len(problems), len(warnings))
	case len(warnings) > 0:
		// Zero exit: the deployment works. The warnings are guarantees that are
		// switched off, which is a decision to make rather than a fault to fix.
		fmt.Printf("reconsyncctl doctor: working, with %d guarantee(s) not enabled\n", len(warnings))
		return nil
	default:
		fmt.Println("reconsyncctl doctor: all checks passed")
		return nil
	}
}

func suffix(detail string) string {
	if detail == "" {
		return ""
	}
	return " (" + detail + ")"
}

func checkSecrets(ok func(string, string), fail func(string, string)) {
	if os.Getenv("RECONSYNC_WEBHOOK_SECRET") == "" {
		fail("webhook secret", "RECONSYNC_WEBHOOK_SECRET is unset — deliveries cannot be signed")
		return
	}
	ok("webhook secret set", "")
}

func checkLicence(ok func(string, string), fail func(string, string), warn func(string, string)) {
	token := os.Getenv("RECONSYNC_LICENCE")
	if token == "" {
		ok("licence", "none configured; every feature available")
		return
	}

	c, err := licence.New(licence.Options{
		Token:     licence.Token(token),
		PublicKey: os.Getenv("RECONSYNC_LICENCE_PUBLIC_KEY"),
	})
	if err != nil {
		// A licence that will not verify stops the server starting, so finding
		// it here is the difference between a failed deploy and a caught typo.
		fail("licence", err.Error())
		return
	}

	st := c.Status()
	switch {
	case st.Expired:
		warn("licence", fmt.Sprintf("expired %d days ago — reports and audit verification "+
			"are withheld. Detection and reversals are unaffected", -st.DaysRemaining))
	case st.DaysRemaining <= 30:
		warn("licence", fmt.Sprintf("expires in %d days (%s)",
			st.DaysRemaining, st.ExpiresAt.Format("2006-01-02")))
	default:
		ok("licence valid", fmt.Sprintf("%d days remaining", st.DaysRemaining))
	}
}

func checkCheckpoints(ok func(string, string), warn func(string, string)) {
	if os.Getenv("RECONSYNC_CHECKPOINT_KEY") == "" {
		warn("audit checkpoints", "RECONSYNC_CHECKPOINT_KEY is unset — the chain verifies "+
			"against itself, but a rewrite of the whole chain would not be detectable")
		return
	}
	ok("audit checkpoints enabled", "")
}

func checkProviders(ok func(string, string), fail func(string, string), warn func(string, string)) {
	path := os.Getenv("RECONSYNC_PROVIDERS_FILE")
	if path == "" {
		warn("provider corroboration", "no providers configured — every orphan rests on "+
			"silence alone, which tops out at 0.70 confidence")
		return
	}

	reg, err := provider.LoadRegistry(path)
	if err != nil {
		// The server refuses to start on this, so catching it here saves a
		// failed deploy.
		fail("provider corroboration", err.Error())
		return
	}
	ok("provider corroboration", fmt.Sprintf("%d rail(s): %v", len(reg.Names()), reg.Names()))
}

// checkDeliveryTargets finds tenants whose reversals would have nowhere to go.
//
// Detection keeps working for such a tenant and the verdict is recorded, but
// nobody is told — the failure is invisible from the inside, which is why it is
// worth a preflight rather than a runtime error.
func checkDeliveryTargets(ctx context.Context, pool *pgxpool.Pool, ok func(string, string), warn func(string, string)) {
	rows, err := pool.Query(ctx, `
		SELECT t.id
		FROM tenants t
		WHERE NOT EXISTS (
			SELECT 1 FROM webhook_endpoints e
			WHERE e.tenant_id = t.id AND e.enabled
		)
		ORDER BY t.id
		LIMIT 20`)
	if err != nil {
		warn("delivery targets", "could not check: "+err.Error())
		return
	}
	defer rows.Close()

	var orphaned []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			warn("delivery targets", "could not read: "+err.Error())
			return
		}
		orphaned = append(orphaned, id)
	}
	if err := rows.Err(); err != nil {
		warn("delivery targets", "could not read: "+err.Error())
		return
	}

	if len(orphaned) > 0 {
		warn("delivery targets", fmt.Sprintf("%d tenant(s) have no enabled endpoint (%v) — "+
			"their reversals are detected and recorded but nobody is told", len(orphaned), orphaned))
		return
	}
	ok("every tenant has a delivery target", "")
}
