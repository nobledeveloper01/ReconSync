package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"

	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func rulesList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rules list", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	found, err := store.NewPostgres(pool).ListRules(ctx, *tenant)
	if err != nil {
		return err
	}
	if len(found) == 0 {
		fmt.Printf("no rules for %s — every transaction uses the %s default\n",
			*tenant, rules.DefaultWindow)
		fmt.Printf("\nAdd one:  reconsyncctl rules create --tenant %s --type transfer --window 120\n", *tenant)
		return nil
	}

	fmt.Printf("%-6s  %-6s  %-12s  %-10s  %-4s  %-8s  %-12s  %s\n",
		"ID", "PRIO", "TYPE", "PROVIDER", "CUR", "WINDOW", "ACTION", "ENABLED")
	for _, r := range found {
		fmt.Printf("%-6d  %-6d  %-12s  %-10s  %-4s  %-8s  %-12s  %t\n",
			r.ID, r.Priority, orAny(r.TransactionType), orAny(r.Provider), orAny(r.Currency),
			strconv.Itoa(r.WindowSeconds)+"s", r.Action, r.Enabled)
	}
	fmt.Printf("\nUnmatched transactions use the %s default.\n", rules.DefaultWindow)
	return nil
}

func orAny(s string) string {
	if s == "" {
		return "*"
	}
	return s
}

func rulesCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rules create", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	window := fs.Int("window", 0, "reconciliation window in seconds (required)")
	txnType := fs.String("type", "", "transaction type to match (empty matches any)")
	provider := fs.String("provider", "", "provider to match (empty matches any)")
	currency := fs.String("currency", "", "currency to match (empty matches any)")
	minAmount := fs.Int64("min-amount", -1, "minimum amount in minor units (-1 for no bound)")
	maxAmount := fs.Int64("max-amount", -1, "maximum amount in minor units (-1 for no bound)")
	priority := fs.Int("priority", 0, "higher wins when several rules match")
	action := fs.String("action", string(rules.ActionAutoReverse), "auto_reverse, alert_only or investigate")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}
	if *window <= 0 {
		return errors.New("--window is required and must be a positive number of seconds")
	}

	act := rules.Action(*action)
	switch act {
	case rules.ActionAutoReverse, rules.ActionAlertOnly, rules.ActionInvestigate:
	default:
		return fmt.Errorf("--action must be auto_reverse, alert_only or investigate, got %q", *action)
	}

	r := &rules.Rule{
		TransactionType: *txnType,
		Provider:        *provider,
		Currency:        *currency,
		WindowSeconds:   *window,
		Action:          act,
		Priority:        *priority,
		Enabled:         true,
	}
	if *minAmount >= 0 {
		r.MinAmountMinor = minAmount
	}
	if *maxAmount >= 0 {
		r.MaxAmountMinor = maxAmount
	}
	if r.MinAmountMinor != nil && r.MaxAmountMinor != nil && *r.MinAmountMinor > *r.MaxAmountMinor {
		return fmt.Errorf("--min-amount %d is greater than --max-amount %d, so the rule can never match",
			*r.MinAmountMinor, *r.MaxAmountMinor)
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	id, err := store.NewPostgres(pool).CreateRule(ctx, *tenant, r)
	if err != nil {
		return err
	}

	fmt.Printf("rule %d created for %s: %ds window, %s\n", id, *tenant, *window, act)
	fmt.Printf("  matches type=%s provider=%s currency=%s\n",
		orAny(*txnType), orAny(*provider), orAny(*currency))
	fmt.Printf("\nTakes effect within %s. No restart needed.\n", rules.DefaultCacheTTL)
	return nil
}

func rulesDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rules delete", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	id := fs.Int64("id", 0, "rule id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}
	if *id <= 0 {
		return errors.New("--id is required")
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := store.NewPostgres(pool).DeleteRule(ctx, *tenant, *id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("no rule %d for tenant %q — run: reconsyncctl rules list --tenant %s", *id, *tenant, *tenant)
		}
		return err
	}
	fmt.Printf("rule %d deleted\n", *id)
	return nil
}
