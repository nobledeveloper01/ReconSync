package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/secret"
	"github.com/nobledeveloper01/ReconSync/pkg/reconsync"
)

// Resolving the signing secret is a path where a mistake is silent. Every
// delivery still goes out, still carries a signature, and every receiver
// rejects it — so the symptom is "our webhooks stopped working" with nothing in
// the logs to say why.

func env(values map[string]string) secret.Option {
	return secret.WithLookup(func(name string) (string, bool) {
		v, ok := values[name]
		return v, ok
	})
}

func TestEachEndpointResolvesItsOwnSecret(t *testing.T) {
	r := secret.New("fallback", env(map[string]string{
		"ACME_SECRET":  "whsec_acme",
		"OTHER_SECRET": "whsec_other",
	}))
	ctx := context.Background()

	for ref, want := range map[string]string{
		"env://ACME_SECRET":  "whsec_acme",
		"env://OTHER_SECRET": "whsec_other",
	} {
		got, err := r.Resolve(ctx, ref)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", ref, err)
		}
		if len(got) != 1 || got[0] != want {
			t.Errorf("Resolve(%s) = %v, want [%s]", ref, got, want)
		}
	}
}

// Every endpoint row written before references were honoured carries the
// default, and enabling this must not have stopped their deliveries.
func TestAnEmptyReferenceFallsBackToTheGlobalSecret(t *testing.T) {
	r := secret.New("whsec_global", env(nil))

	got, err := r.Resolve(context.Background(), "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 1 || got[0] != "whsec_global" {
		t.Errorf("Resolve(\"\") = %v, want [whsec_global]", got)
	}

	// With neither, it says so rather than signing with an empty key — an HMAC
	// with an empty secret is a perfectly valid HMAC that nobody can verify.
	if _, err := secret.New("", env(nil)).Resolve(context.Background(), ""); err == nil {
		t.Error("resolved to something with no reference and no fallback")
	}
}

func TestAnUnresolvableReferenceIsRefusedByName(t *testing.T) {
	r := secret.New("whsec_global", env(map[string]string{"PRESENT": "whsec_present"}))
	ctx := context.Background()

	for _, tc := range []struct{ ref, wants string }{
		// Named, so an operator who has just added an endpoint is told which
		// variable to set rather than that delivery failed.
		{"env://MISSING", "MISSING"},
		{"env://", "names no environment variable"},
		// Refused rather than treated as a literal secret: a typo would
		// otherwise become the signing key, and every delivery would be signed
		// with a value the receiver has never seen while looking like it worked.
		{"whsec_pasted_by_mistake", "not a supported reference"},
		{"kms://arn:aws:kms:eu-west-1:1234:key/abc", "not a supported reference"},
	} {
		_, err := r.Resolve(ctx, tc.ref)
		if err == nil {
			t.Errorf("Resolve(%q) succeeded", tc.ref)
			continue
		}
		if !strings.Contains(err.Error(), tc.wants) {
			t.Errorf("Resolve(%q) said %q, want it to mention %q", tc.ref, err, tc.wants)
		}
	}

	// The fallback must not paper over a broken reference. Falling back there
	// would sign with the wrong secret and look like success.
	if _, err := r.Resolve(ctx, "env://MISSING"); err == nil {
		t.Error("a missing variable fell back to the global secret")
	}
}

// Rotation, end to end: a payload signed with both secrets verifies for a
// receiver holding either one, so the two sides can be changed on different
// days by different people.
func TestRotationSignsWithBothSecrets(t *testing.T) {
	r := secret.New("", env(map[string]string{
		"WEBHOOK_SECRET_NEW": "whsec_new",
		"WEBHOOK_SECRET_OLD": "whsec_old",
	}))

	secrets, err := r.Resolve(context.Background(), "env://WEBHOOK_SECRET_NEW,env://WEBHOOK_SECRET_OLD")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(secrets) != 2 || secrets[0] != "whsec_new" {
		t.Fatalf("Resolve = %v, want the new secret first", secrets)
	}

	body := []byte(`{"event":"reversal.triggered"}`)
	now := time.Now()
	header := reconsync.SignWith(secrets, now, body)

	// A receiver that has not been touched yet.
	if err := reconsync.Verify("whsec_old", header, body, now, reconsync.DefaultTolerance); err != nil {
		t.Errorf("a receiver on the old secret was broken by the rotation: %v", err)
	}
	// A receiver that has already moved.
	if err := reconsync.Verify("whsec_new", header, body, now, reconsync.DefaultTolerance); err != nil {
		t.Errorf("a receiver on the new secret cannot verify: %v", err)
	}
	// And one that holds neither still cannot.
	if err := reconsync.Verify("whsec_attacker", header, body, now, reconsync.DefaultTolerance); err == nil {
		t.Error("an unrelated secret verified")
	}

	// The other direction: a receiver holding both, a sender on one.
	single := reconsync.Sign("whsec_new", now, body)
	if err := reconsync.VerifyAny([]string{"whsec_old", "whsec_new"}, single, body, now, reconsync.DefaultTolerance); err != nil {
		t.Errorf("a receiver holding both could not verify a single-secret signature: %v", err)
	}
}

// A header carrying several signatures must not be reduced to whichever one
// happens to come last, or the sender's ordering would decide whether the
// receiver works.
func TestEverySignatureInTheHeaderIsConsidered(t *testing.T) {
	body := []byte(`{"event":"reversal.triggered"}`)
	now := time.Now()

	header := reconsync.SignWith([]string{"whsec_a", "whsec_b", "whsec_c"}, now, body)
	if strings.Count(header, "v1=") != 3 {
		t.Fatalf("header = %q, want three signatures", header)
	}

	for _, s := range []string{"whsec_a", "whsec_b", "whsec_c"} {
		if err := reconsync.Verify(s, header, body, now, reconsync.DefaultTolerance); err != nil {
			t.Errorf("%s did not verify: %v", s, err)
		}
	}
	if err := reconsync.Verify("whsec_d", header, body, now, reconsync.DefaultTolerance); err == nil {
		t.Error("a secret that signed nothing verified")
	}
}
