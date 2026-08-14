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
  reconsyncctl probe --url URL                 exit 0 if an endpoint is healthy
  reconsyncctl migrate up                      apply pending migrations
  reconsyncctl migrate status                  what has run, what has not
  reconsyncctl migrate baseline                mark all applied, for an existing database
  reconsyncctl tenant create --id ID [--name N] [--env test|live]
  reconsyncctl keys create --tenant ID [--env test|live] [--scopes a,b]
  reconsyncctl endpoints create --tenant ID --url URL [--events a,b]
                               [--allow-private] [--allow-insecure]  (dev only)
  reconsyncctl endpoints list --tenant ID
  reconsyncctl endpoints test --tenant ID --id ENDPOINT_ID
  reconsyncctl rules create --tenant ID --window SECONDS [--type T] [--provider P]
  reconsyncctl rules list --tenant ID
  reconsyncctl rules delete --tenant ID --id RULE_ID
  reconsyncctl licence keygen                  mint the licence signing key (vendor)
  reconsyncctl licence issue --customer NAME --months N   (vendor)
  reconsyncctl licence show                    what the configured licence says
  reconsyncctl checkpoints keygen              mint an audit signing key
  reconsyncctl checkpoints list --tenant ID    signed chain heads, as JSON to archive
  reconsyncctl checkpoints verify --tenant ID [--public-key KEY]

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

	case "probe":
		return probe(ctx, args[1:])

	case "migrate":
		return dispatch(ctx, "migrate", args[1:], map[string]subcommand{
			"up":       migrateUp,
			"status":   migrateStatus,
			"baseline": migrateBaseline,
		})

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

	case "rules":
		return dispatch(ctx, "rules", args[1:], map[string]subcommand{
			"create": rulesCreate,
			"list":   rulesList,
			"delete": rulesDelete,
		})

	case "licence":
		return dispatch(ctx, "licence", args[1:], map[string]subcommand{
			"keygen": licenceKeygen,
			"issue":  licenceIssue,
			"show":   licenceShow,
		})

	case "checkpoints":
		return dispatch(ctx, "checkpoints", args[1:], map[string]subcommand{
			"keygen": checkpointsKeygen,
			"list":   checkpointsList,
			"verify": checkpointsVerify,
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
	scopes := fs.String("scopes", "", "comma-separated scopes; empty means full access")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}

	granted, err := parseScopes(*scopes)
	if err != nil {
		return err
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
	if err := store.NewPostgres(pool).CreateAPIKey(ctx, *tenant, keyID, key, granted); err != nil {
		return err
	}

	// Shown once and never again: only the prefix and the hash are stored.
	fmt.Printf("key id: %s\nprefix: %s\nsecret: %s\n\n", keyID, key.Prefix, key.Secret)
	fmt.Println("Store the secret now — it cannot be retrieved later.")
	return nil
}

// parseScopes validates the requested scopes.
//
// An unknown scope is refused rather than stored: a key holding "endpoint:write"
// instead of "endpoints:write" would be silently denied everything it was meant
// to do, and the operator would debug the endpoint rather than the typo.
func parseScopes(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	known := map[string]struct{}{
		auth.ScopeEventsWrite:    {},
		auth.ScopeReportsRead:    {},
		auth.ScopeEndpointsWrite: {},
	}

	var out []string
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := known[s]; !ok {
			return nil, fmt.Errorf("unknown scope %q; valid scopes are %s, %s, %s",
				s, auth.ScopeEventsWrite, auth.ScopeReportsRead, auth.ScopeEndpointsWrite)
		}
		out = append(out, s)
	}
	return out, nil
}
