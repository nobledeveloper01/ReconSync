// Package secret resolves an endpoint's signing secret from the reference
// stored beside it.
//
// The distinction the whole design rests on: an endpoint row holds a
// *reference*, never a secret. A reference can be dumped, reviewed, backed up
// and put in a support ticket; a secret cannot. Resolving it is done at
// delivery time, by this package, from somewhere the database cannot see.
package secret

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// Scheme prefixes a reference understands.
const (
	// EnvScheme reads an environment variable. The only scheme built, because
	// it is the one that works everywhere — a KMS reference would be a second
	// implementation to keep correct for a deployment nobody has yet.
	EnvScheme = "env://"
)

// Resolver turns a reference into the secrets an endpoint signs with.
type Resolver struct {
	lookup   func(string) (string, bool)
	fallback string
}

// Option configures a Resolver.
type Option func(*Resolver)

// WithLookup replaces the environment lookup, for tests.
func WithLookup(f func(string) (string, bool)) Option {
	return func(r *Resolver) {
		if f != nil {
			r.lookup = f
		}
	}
}

// New builds a resolver.
//
// fallback is the secret used when an endpoint carries no reference at all,
// which is every row written before references were honoured. Without it,
// enabling this would have stopped delivery for every existing deployment —
// and a reconciliation service that silently stops delivering reversals is the
// worst failure this system has.
func New(fallback string, opts ...Option) *Resolver {
	r := &Resolver{lookup: os.LookupEnv, fallback: fallback}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Resolve returns the secrets for a reference, the signing one first.
//
// More than one is returned during a rotation. A reference of
// "env://NEW,env://OLD" signs with both, so the payload carries a signature
// each — a receiver holding either secret verifies it, and nobody has to
// coordinate a simultaneous cutover across two systems.
func (r *Resolver) Resolve(_ context.Context, ref string) ([]string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		if r.fallback == "" {
			return nil, fmt.Errorf("secret: this endpoint has no secret reference and no default is configured")
		}
		return []string{r.fallback}, nil
	}

	parts := strings.Split(ref, ",")
	secrets := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		value, err := r.resolveOne(part)
		if err != nil {
			return nil, err
		}
		secrets = append(secrets, value)
	}

	if len(secrets) == 0 {
		return nil, fmt.Errorf("secret: %q names no secret", ref)
	}
	return secrets, nil
}

func (r *Resolver) resolveOne(ref string) (string, error) {
	if !strings.HasPrefix(ref, EnvScheme) {
		// Refused rather than treated as a literal secret. Accepting an
		// unknown scheme as plaintext would mean a typo in a reference becomes
		// the signing key, and every delivery would be signed with a value the
		// receiver has never seen — while looking like it worked.
		return "", fmt.Errorf("secret: %q is not a supported reference; expected %sNAME", ref, EnvScheme)
	}

	name := strings.TrimPrefix(ref, EnvScheme)
	if name == "" {
		return "", fmt.Errorf("secret: %q names no environment variable", ref)
	}

	value, ok := r.lookup(name)
	if !ok || value == "" {
		// Named explicitly. An operator who has just added an endpoint with a
		// new reference needs to be told which variable is missing, not that
		// delivery failed.
		return "", fmt.Errorf("secret: %s is not set, so %q cannot be resolved", name, ref)
	}
	return value, nil
}
