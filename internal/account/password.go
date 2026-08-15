package account

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// argon2id, with the same reasoning as the API keys: it is memory-hard, so an
// attacker with a stolen table cannot trade GPUs for speed the way they can
// against SHA-2 or bcrypt at low cost.
const (
	argonTime    = 1
	argonMemory  = 64 * 1024 // 64 MiB
	argonThreads = 4
	argonKeyLen  = 32
	saltLen      = 16
)

// HashPassword returns an encoded argon2id hash, salt included.
func HashPassword(pw string) (string, error) {
	if err := ValidatePassword(pw); err != nil {
		return "", err
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("account: generate salt: %w", err)
	}
	key := argon2.IDKey([]byte(pw), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	// Self-describing, so the cost can be raised later without invalidating
	// every existing password: an old hash still says how it was made.
	return fmt.Sprintf("argon2id$%d$%d$%d$%s$%s",
		argonTime, argonMemory, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

// VerifyPassword checks a password against an encoded hash.
func VerifyPassword(encoded, pw string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "argon2id" {
		return false
	}

	var t, m, p int
	if _, err := fmt.Sscanf(parts[1]+" "+parts[2]+" "+parts[3], "%d %d %d", &t, &m, &p); err != nil {
		return false
	}
	// Bounded before use. These come out of the database, and a row claiming a
	// gigabyte of memory would have this process spend it — or, truncated into
	// a uint32, spend some unrelated amount instead.
	if t < 1 || t > 16 || m < 8*1024 || m > 1024*1024 || p < 1 || p > 16 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false
	}

	//nolint:gosec // t, m and p are range-checked above; len(want) is a decoded length
	got := argon2.IDKey([]byte(pw), salt, uint32(t), uint32(m), uint8(p), uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

// A dummy hash, verified against when no user matches.
//
// Without it, a request for an unknown address returns in microseconds while a
// real one spends 64 MiB of hashing — a timing difference big enough to
// enumerate every account from a laptop.
var dummyHash string

func init() {
	h, err := HashPassword(strings.Repeat("x", MinPasswordLength))
	if err != nil {
		panic("account: cannot build dummy hash: " + err.Error())
	}
	dummyHash = h
}

// WasteTime performs the same work a real verification would.
func WasteTime() { _ = VerifyPassword(dummyHash, "not the password") }

// NewToken mints a session or reset token and returns it with its hash.
//
// Only the hash is ever stored, so a database disclosure yields nothing that
// can be presented as a session — the same reasoning as the API key table.
func NewToken() (token, hash string, err error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", "", fmt.Errorf("account: generate token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(b[:])
	return token, HashToken(token), nil
}

// HashToken is the stored form of a token.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ErrWeakPassword is returned when a new password fails validation.
var ErrWeakPassword = errors.New("account: password is too weak")
