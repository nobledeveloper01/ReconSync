package audit

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// A hash chain proves nobody edited a record in place. It does not prove nobody
// rewrote the whole thing.
//
// Someone with write access to the database can delete every row, reinsert their
// preferred history, and recompute every hash from the start. The result is a
// perfectly self-consistent chain that verifies. Nothing inside the database can
// detect that, because everything inside the database is exactly what the
// attacker controls.
//
// A checkpoint is a signature over the chain head, made with a key that is not
// in the database and published somewhere the attacker does not own. Rewriting
// history now means producing a chain whose head hash matches a signature they
// cannot forge — which is the point.

// Checkpoint is a signed statement that a tenant's chain reached a given hash at
// a given sequence.
type Checkpoint struct {
	TenantID string    `json:"tenant_id"`
	Seq      int64     `json:"seq"`
	Hash     string    `json:"hash"`
	TakenAt  time.Time `json:"taken_at"`

	// Signature is over the fields above, base64. Verifiable by anyone holding
	// the public key: that is what makes publishing it worth anything.
	Signature string `json:"signature,omitempty"`

	// PublicKey identifies which key signed, so rotation does not invalidate
	// older checkpoints.
	PublicKey string `json:"public_key,omitempty"`
}

// signedContent is the exact shape that gets signed. Written out explicitly for
// the same reason as hashedContent: adding a Go field must not silently
// invalidate every signature ever made.
type signedContent struct {
	TenantID string `json:"tenant_id"`
	Seq      int64  `json:"seq"`
	Hash     string `json:"hash"`
	TakenAt  string `json:"taken_at"`
}

// ErrNoSigningKey means checkpoints are not configured.
var ErrNoSigningKey = errors.New("audit: no checkpoint signing key configured")

// Signer signs checkpoints.
//
// Ed25519 rather than an HMAC deliberately. An HMAC would let us verify our own
// checkpoints, but a customer or regulator could only verify one by asking us
// for the secret — at which point they could forge checkpoints too, and the
// signature proves nothing to the party that most needs it. With a public key
// they verify independently, and we cannot quietly re-sign a rewritten history.
type Signer struct {
	priv ed25519.PrivateKey
}

// NewSigner builds a Signer from a hex or base64 encoded Ed25519 private key.
func NewSigner(encoded string) (*Signer, error) {
	if encoded == "" {
		return nil, ErrNoSigningKey
	}
	raw, err := decodeKey(encoded)
	if err != nil {
		return nil, err
	}

	switch len(raw) {
	case ed25519.PrivateKeySize:
		return &Signer{priv: ed25519.PrivateKey(raw)}, nil
	case ed25519.SeedSize:
		// A 32-byte seed is what most key generators hand you.
		return &Signer{priv: ed25519.NewKeyFromSeed(raw)}, nil
	default:
		return nil, fmt.Errorf("audit: signing key must be %d or %d bytes, got %d",
			ed25519.SeedSize, ed25519.PrivateKeySize, len(raw))
	}
}

// GenerateKey returns a new hex-encoded seed, for `reconsyncctl checkpoints keygen`.
func GenerateKey() (seed, public string, err error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return "", "", fmt.Errorf("audit: generate key: %w", err)
	}
	return hex.EncodeToString(priv.Seed()), base64.StdEncoding.EncodeToString(pub), nil
}

// PublicKey is the base64 verifying key. Publish it: a checkpoint nobody can
// check is decoration.
func (s *Signer) PublicKey() string {
	return base64.StdEncoding.EncodeToString(s.priv.Public().(ed25519.PublicKey))
}

// Sign fills in the signature and public key.
func (s *Signer) Sign(c Checkpoint) (Checkpoint, error) {
	msg, err := checkpointMessage(c)
	if err != nil {
		return Checkpoint{}, err
	}
	c.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(s.priv, msg))
	c.PublicKey = s.PublicKey()
	return c, nil
}

// VerifyCheckpoint checks a checkpoint against a public key.
//
// Standalone and dependency-free on purpose: this is the function a customer
// reimplements in ten lines of their own language to check us without our code.
func VerifyCheckpoint(c Checkpoint, publicKey string) error {
	pub, err := base64.StdEncoding.DecodeString(publicKey)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return errors.New("audit: public key must be a base64 Ed25519 key")
	}
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil {
		return errors.New("audit: signature is not valid base64")
	}
	msg, err := checkpointMessage(c)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, msg, sig) {
		return errors.New("audit: checkpoint signature does not verify")
	}
	return nil
}

// checkpointMessage builds the bytes that get signed.
//
// TakenAt is truncated to storage precision for the same reason record hashes
// are: a signature over nanoseconds the database rounds away stops verifying
// the moment the checkpoint is read back, which would make every checkpoint
// written on Linux look like evidence of tampering.
func checkpointMessage(c Checkpoint) ([]byte, error) {
	encoded, err := json.Marshal(signedContent{
		TenantID: c.TenantID,
		Seq:      c.Seq,
		Hash:     c.Hash,
		TakenAt:  c.TakenAt.UTC().Truncate(StoragePrecision).Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, fmt.Errorf("audit: encode checkpoint: %w", err)
	}
	return encoded, nil
}

func decodeKey(encoded string) ([]byte, error) {
	if raw, err := hex.DecodeString(encoded); err == nil {
		return raw, nil
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("audit: signing key must be hex or base64")
	}
	return raw, nil
}

// CheckpointCheck is what verification says about the signed history.
type CheckpointCheck struct {
	// Checked is false when no checkpoint covers this chain yet, which is not
	// a pass — it means the strongest guarantee is simply absent.
	Checked bool `json:"checked"`

	Seq       int64     `json:"seq,omitempty"`
	TakenAt   time.Time `json:"taken_at,omitempty"`
	Matches   bool      `json:"matches"`
	PublicKey string    `json:"public_key,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

// VerifyAgainstCheckpoint re-walks the chain up to the checkpoint's sequence and
// compares the hash it arrives at with the one that was signed.
//
// This is the check that survives a full rewrite: the attacker controls every
// record, so they can make VerifyChain pass, but the head hash they arrive at
// will not be the one in a signature they cannot produce.
func VerifyAgainstCheckpoint(records []Record, c Checkpoint, publicKey string) CheckpointCheck {
	out := CheckpointCheck{Checked: true, Seq: c.Seq, TakenAt: c.TakenAt.UTC(), PublicKey: c.PublicKey}

	if err := VerifyCheckpoint(c, publicKey); err != nil {
		out.Reason = "the checkpoint itself does not verify: " + err.Error()
		return out
	}

	prevHash := ""
	var reached string
	for _, r := range records {
		if r.Seq > c.Seq {
			break
		}
		want, err := ComputeHash(r, prevHash)
		if err != nil {
			out.Reason = "record could not be hashed: " + err.Error()
			return out
		}
		prevHash = want
		if r.Seq == c.Seq {
			reached = want
		}
	}

	switch {
	case reached == "":
		// The signed sequence is not in the chain at all. Records were removed
		// from the end, which a self-consistent rewrite would look exactly like.
		out.Reason = fmt.Sprintf("the chain does not reach seq %d, which was signed on %s; records were removed",
			c.Seq, c.TakenAt.UTC().Format(time.RFC3339))
	case reached != c.Hash:
		out.Reason = fmt.Sprintf("the chain reaches a different hash at seq %d than the one signed on %s; history was rewritten",
			c.Seq, c.TakenAt.UTC().Format(time.RFC3339))
	default:
		out.Matches = true
	}
	return out
}
