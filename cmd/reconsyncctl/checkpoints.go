package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// checkpointsKeygen mints an Ed25519 key for signing chain heads.
func checkpointsKeygen(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("checkpoints keygen", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	seed, public, err := audit.GenerateKey()
	if err != nil {
		return err
	}

	// The private key goes to stdout once and is never stored by us. Printing
	// it to a terminal is the least bad option for a self-hosted tool with no
	// KMS assumption; the warning is there because it will end up in a shell
	// history otherwise.
	fmt.Printf("RECONSYNC_CHECKPOINT_KEY=%s\n", seed)
	fmt.Printf("public key: %s\n", public)
	fmt.Fprint(os.Stderr, `
Set the private key on the ReconSync process and publish the public key.

Publishing matters more than it sounds. A checkpoint is only evidence to someone
who can verify it without trusting you — put the public key somewhere your
auditors and customers can read it, and keep exported checkpoints somewhere this
database cannot reach.
`)
	return nil
}

// checkpointsList prints a tenant's signed heads as JSON, for archiving.
func checkpointsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("checkpoints list", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	limit := fs.Int("limit", 100, "how many to print, newest first")
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

	checkpoints, err := store.NewPostgres(pool).ListCheckpoints(ctx, *tenant, *limit)
	if err != nil {
		return err
	}
	if len(checkpoints) == 0 {
		return fmt.Errorf("no checkpoints for tenant %q — is RECONSYNC_CHECKPOINT_KEY set on the server?", *tenant)
	}

	// JSON rather than a table: this output is meant to be redirected to a file
	// and kept, not read.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(checkpoints)
}

// checkpointsVerify checks a signed head against the chain, without the server.
func checkpointsVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("checkpoints verify", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	key := fs.String("public-key", "", "base64 public key to verify against")
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
	db := store.NewPostgres(pool)

	latest, err := db.LatestCheckpoint(ctx, *tenant)
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("no checkpoints for tenant %q", *tenant)
	}
	if err != nil {
		return err
	}

	// Defaulting to the key inside the checkpoint is convenient and proves
	// almost nothing — an attacker who rewrote the chain would sign it with
	// their own key and store that too. Say so rather than let it read as a
	// pass.
	publicKey := *key
	trusted := true
	if publicKey == "" {
		publicKey = latest.PublicKey
		trusted = false
	}

	records, err := db.ListAudit(ctx, *tenant, 0)
	if err != nil {
		return err
	}

	chain := audit.VerifyChain(*tenant, records)
	check := audit.VerifyAgainstCheckpoint(records, *latest, publicKey)

	fmt.Printf("chain:      %d records, verified=%v\n", chain.Records, chain.Verified)
	if !chain.Verified {
		fmt.Printf("            broken at seq %d: %s\n", chain.BrokenAt, chain.Reason)
	}
	fmt.Printf("checkpoint: seq %d, taken %s, matches=%v\n",
		check.Seq, check.TakenAt.Format("2006-01-02T15:04:05Z"), check.Matches)
	if check.Reason != "" {
		fmt.Printf("            %s\n", check.Reason)
	}
	fmt.Printf("public key: %s\n", publicKey)
	if !trusted {
		fmt.Fprint(os.Stderr, "\nwarning: verified against the key stored beside the checkpoint. "+
			"Pass --public-key with the key you published to make this mean something.\n")
	}

	if !chain.Verified || !check.Matches {
		return errors.New("the audit chain does not verify")
	}
	return nil
}
