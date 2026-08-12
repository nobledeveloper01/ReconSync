// Command reconsyncctl is the admin CLI (§6.3).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

const usage = `reconsyncctl — ReconSync admin CLI

Usage:
  reconsyncctl doctor                          check the deployment is sound
  reconsyncctl tenant create --id ID [--name N] [--env test|live]
  reconsyncctl keys create --tenant ID [--env test|live]
  reconsyncctl endpoints create --tenant ID --url URL [--events a,b]
  reconsyncctl endpoints list --tenant ID
  reconsyncctl endpoints test --tenant ID --id ENDPOINT_ID

Reads RECONSYNC_DATABASE_URL from the environment.
"endpoints test" also needs RECONSYNC_WEBHOOK_SECRET to sign the payload.
`

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("no command given")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Nouns route separately from their verbs, so a missing verb reports the
	// verb as missing rather than claiming the noun is unknown.
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil

	case "doctor":
		return doctor(ctx)

	case "tenant":
		return dispatch(ctx, "tenant", args[1:], map[string]subcommand{
			"create": tenantCreate,
		})

	case "keys":
		return dispatch(ctx, "keys", args[1:], map[string]subcommand{
			"create": keysCreate,
		})

	case "endpoints":
		return dispatch(ctx, "endpoints", args[1:], map[string]subcommand{
			"create": endpointsCreate,
			"list":   endpointsList,
			"test":   endpointsTest,
		})
	}

	fmt.Fprint(os.Stderr, usage)
	return fmt.Errorf("unknown command %q", args[0])
}

type subcommand func(ctx context.Context, args []string) error

// dispatch routes a noun's verb, naming the valid verbs when one is missing or
// unrecognised.
func dispatch(ctx context.Context, noun string, args []string, verbs map[string]subcommand) error {
	valid := make([]string, 0, len(verbs))
	for v := range verbs {
		valid = append(valid, v)
	}
	sort.Strings(valid)

	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("%q needs a subcommand: %s", noun, strings.Join(valid, ", "))
	}
	fn, ok := verbs[args[0]]
	if !ok {
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown %s subcommand %q: expected %s", noun, args[0], strings.Join(valid, ", "))
	}
	return fn(ctx, args[1:])
}

func connect(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("RECONSYNC_DATABASE_URL")
	if url == "" {
		return nil, errors.New("RECONSYNC_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return pool, nil
}

// doctor checks the things that make an install fail in ways the operator
// cannot diagnose from the error alone (§6.3).
func doctor(ctx context.Context) error {
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database unreachable: %w", err)
	}
	fmt.Println("✓ database reachable")

	tables := []string{"tenants", "api_keys", "transactions", "pending_credits",
		"reconciliation_rules", "webhook_endpoints", "webhook_deliveries", "audit_records"}
	for _, table := range tables {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check table %s: %w", table, err)
		}
		if !exists {
			return fmt.Errorf("table %q is missing — run the migrations in migrations/", table)
		}
	}
	fmt.Printf("✓ schema present (%d tables)\n", len(tables))

	// Clock skew breaks webhook signature verification with an error message
	// that tells the operator nothing, so it is worth checking explicitly.
	var dbNow time.Time
	if err := pool.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
		return fmt.Errorf("read database time: %w", err)
	}
	skew := time.Since(dbNow)
	if skew < 0 {
		skew = -skew
	}
	if skew > 5*time.Second {
		return fmt.Errorf("clock skew between this host and the database is %s — "+
			"webhook signatures and reconciliation windows will misbehave", skew.Round(time.Millisecond))
	}
	fmt.Printf("✓ clock skew %s\n", skew.Round(time.Millisecond))

	fmt.Println("\nreconsyncctl doctor: all checks passed")
	return nil
}

func tenantCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenant create", flag.ContinueOnError)
	id := fs.String("id", "", "tenant id, e.g. tnt_acme")
	name := fs.String("name", "", "display name (defaults to the id)")
	env := fs.String("env", "test", "test or live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	if *env != "test" && *env != "live" {
		return fmt.Errorf("--env must be test or live, got %q", *env)
	}
	if *name == "" {
		*name = *id
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := store.NewPostgres(pool).EnsureTenant(ctx, *id, *name, *env); err != nil {
		return err
	}
	fmt.Printf("tenant %s ready (%s)\n", *id, *env)
	return nil
}

func keysCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("keys create", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	env := fs.String("env", "test", "test or live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}

	environment := auth.Environment(*env)
	if !environment.Valid() {
		return fmt.Errorf("--env must be test or live, got %q", *env)
	}

	key, err := auth.Generate(environment)
	if err != nil {
		return err
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	keyID := fmt.Sprintf("key_%d", time.Now().UnixNano())
	if err := store.NewPostgres(pool).CreateAPIKey(ctx, *tenant, keyID, key, nil); err != nil {
		return err
	}

	// Shown once and never again: only the prefix and the hash are stored.
	fmt.Printf("key id: %s\nprefix: %s\nsecret: %s\n\n", keyID, key.Prefix, key.Secret)
	fmt.Println("Store the secret now — it cannot be retrieved later.")
	return nil
}
