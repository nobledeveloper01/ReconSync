package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/licence"
)

// licenceKeygen mints the signing key. Vendor side, run once.
func licenceKeygen(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("licence keygen", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}
	fmt.Printf("RECONSYNC_LICENCE_SIGNING_KEY=%s\n", hex.EncodeToString(priv.Seed()))
	fmt.Printf("RECONSYNC_LICENCE_PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(pub))
	fmt.Fprint(os.Stderr, `
Keep the signing key. It never leaves your side: it is what mints licences.
The public key ships with the binary and only verifies them.
`)
	return nil
}

// licenceIssue signs a licence for a customer. Vendor side.
func licenceIssue(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("licence issue", flag.ContinueOnError)
	customer := fs.String("customer", "", "who the licence is for")
	plan := fs.String("plan", "", "plan name, recorded in the licence")
	months := fs.Int("months", 0, "how long it runs")
	days := fs.Int("days", 0, "how long it runs, in days; negative issues an already-expired licence, for testing what a customer will see")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *customer == "" {
		return errors.New("--customer is required")
	}
	if *months == 0 && *days == 0 {
		return errors.New("--months or --days is required")
	}

	seed := os.Getenv("RECONSYNC_LICENCE_SIGNING_KEY")
	if seed == "" {
		return errors.New("RECONSYNC_LICENCE_SIGNING_KEY is required — run: reconsyncctl licence keygen")
	}
	raw, err := hex.DecodeString(seed)
	if err != nil || len(raw) != ed25519.SeedSize {
		return fmt.Errorf("RECONSYNC_LICENCE_SIGNING_KEY must be a %d-byte hex seed", ed25519.SeedSize)
	}

	now := time.Now().UTC()
	expires := now.AddDate(0, *months, *days)

	token, err := licence.Issue(licence.Licence{
		Customer:  *customer,
		Plan:      *plan,
		IssuedAt:  now,
		ExpiresAt: expires,
	}, ed25519.NewKeyFromSeed(raw))
	if err != nil {
		return err
	}

	fmt.Printf("RECONSYNC_LICENCE=%s\n", token)
	fmt.Fprintf(os.Stderr, "\nissued to %s, expires %s (%d days)\n",
		*customer, expires.Format("2006-01-02"), int(time.Until(expires).Hours()/24))
	return nil
}

// licenceShow reports what a token says, without a server.
func licenceShow(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("licence show", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	token := os.Getenv("RECONSYNC_LICENCE")
	if token == "" {
		fmt.Println("no licence configured; every feature is available")
		return nil
	}
	publicKey := os.Getenv("RECONSYNC_LICENCE_PUBLIC_KEY")
	if publicKey == "" {
		return errors.New("RECONSYNC_LICENCE_PUBLIC_KEY is required to verify the token")
	}

	checker, err := licence.New(licence.Options{
		Token:     licence.Token(token),
		PublicKey: publicKey,
	})
	if err != nil {
		return err
	}
	st := checker.Status()

	fmt.Printf("customer:  %s\n", st.Customer)
	if st.Plan != "" {
		fmt.Printf("plan:      %s\n", st.Plan)
	}
	fmt.Printf("expires:   %s\n", st.ExpiresAt.Format("2006-01-02"))
	fmt.Printf("remaining: %d days\n", st.DaysRemaining)
	if st.Notice != "" {
		fmt.Printf("\n%s\n", st.Notice)
	}
	// Non-zero on expiry so this is usable as a monitoring check.
	if st.Expired {
		return errors.New("licence has expired")
	}
	return nil
}
