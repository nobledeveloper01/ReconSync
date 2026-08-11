// Package auth issues and verifies API keys (§8.2).
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Environment scopes a key. A leaked test key can do nothing to live data.
type Environment string

const (
	EnvTest Environment = "test"
	EnvLive Environment = "live"
)

func (e Environment) Valid() bool { return e == EnvTest || e == EnvLive }

const (
	// keyBytes is the entropy behind the secret half of a key.
	keyBytes = 24

	// PrefixLen is how much of the key is stored in plaintext for lookup and
	// dashboard display: "rs_live_" plus four characters.
	PrefixLen = 12
)

// argon2id parameters. Deliberately at the cost of an interactive login rather
// than a per-request check — see the cache in verifier.go, which is what keeps
// this off the hot path.
const (
	argonTime    = 1
	argonMemory  = 64 * 1024 // 64 MiB
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16
)

var (
	ErrMalformedKey  = errors.New("auth: malformed api key")
	ErrMalformedHash = errors.New("auth: malformed key hash")
)

// Key is a freshly issued credential. The Secret is returned once and never
// again — only Prefix and Hash are stored.
type Key struct {
	Secret string // rs_live_9f2a7c... shown to the user exactly once
	Prefix string // rs_live_9f2a — plaintext, for lookup
	Hash   string // argon2id PHC string
}

// Generate issues a new API key for an environment.
func Generate(env Environment) (Key, error) {
	if !env.Valid() {
		return Key{}, fmt.Errorf("auth: unknown environment %q", env)
	}

	raw := make([]byte, keyBytes)
	if _, err := rand.Read(raw); err != nil {
		return Key{}, fmt.Errorf("auth: read entropy: %w", err)
	}

	secret := fmt.Sprintf("rs_%s_%s", env, base64.RawURLEncoding.EncodeToString(raw))
	hash, err := Hash(secret)
	if err != nil {
		return Key{}, err
	}
	return Key{Secret: secret, Prefix: Prefix(secret), Hash: hash}, nil
}

// Prefix returns the lookup prefix of a key. Short keys return themselves, so a
// malformed token still produces a miss rather than a panic.
func Prefix(secret string) string {
	if len(secret) <= PrefixLen {
		return secret
	}
	return secret[:PrefixLen]
}

// Environment reports which environment a key belongs to, from its own text.
func EnvironmentOf(secret string) (Environment, bool) {
	parts := strings.SplitN(secret, "_", 3)
	if len(parts) != 3 || parts[0] != "rs" {
		return "", false
	}
	env := Environment(parts[1])
	return env, env.Valid()
}

// Hash derives an argon2id PHC string for a key.
func Hash(secret string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: read salt: %w", err)
	}
	sum := argon2.IDKey([]byte(secret), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(sum),
	), nil
}

// Verify checks a presented key against a stored hash in constant time.
func Verify(encodedHash, presented string) (bool, error) {
	params, salt, want, err := decodeHash(encodedHash)
	if err != nil {
		return false, err
	}
	// The digest comes from the database, so its length is untrusted. We only
	// ever emit argonKeyLen bytes; anything else is a malformed hash, and
	// checking that avoids widening an unbounded length into uint32.
	if len(want) != argonKeyLen {
		return false, ErrMalformedHash
	}

	got := argon2.IDKey([]byte(presented), salt, params.time, params.memory, params.threads, argonKeyLen)
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

type argonParams struct {
	memory  uint32
	time    uint32
	threads uint8
}

func decodeHash(encoded string) (argonParams, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return argonParams{}, nil, nil, ErrMalformedHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return argonParams{}, nil, nil, ErrMalformedHash
	}
	if version != argon2.Version {
		return argonParams{}, nil, nil, fmt.Errorf("auth: unsupported argon2 version %d", version)
	}

	var p argonParams
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return argonParams{}, nil, nil, ErrMalformedHash
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return argonParams{}, nil, nil, ErrMalformedHash
	}
	sum, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return argonParams{}, nil, nil, ErrMalformedHash
	}
	return p, salt, sum, nil
}
