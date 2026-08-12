// Command reconsync-echo is a reference webhook receiver.
//
// It does the one thing every integration must get right: verify the signature
// before trusting the payload. Run it to see deliveries arrive, or read it as
// the worked example of what your own handler needs to do.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

func main() {
	addr := envOr("ECHO_ADDR", "127.0.0.1:8099")
	secret := os.Getenv("RECONSYNC_WEBHOOK_SECRET")
	if secret == "" {
		fmt.Fprintln(os.Stderr, "error: RECONSYNC_WEBHOOK_SECRET is required — it must match the value the server signs with")
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		defer func() { _ = r.Body.Close() }()

		// Verify first, parse second. A payload that has not been verified is
		// attacker-controlled input, and acting on it is how a forged reversal
		// becomes a real payment.
		if err := webhook.Verify(secret, r.Header.Get(webhook.SignatureHeader), body, time.Now(), webhook.DefaultTolerance); err != nil {
			fmt.Printf("REJECTED  %s: %v\n", r.Header.Get(webhook.EventHeader), err)
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		// Events come in more than one shape — some concern a transaction,
		// some the event stream itself — so this prints what actually arrived
		// rather than forcing every payload through one struct and inventing
		// zero values for the fields that do not apply.
		var head struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(body, &head); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "  ", "  "); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		fmt.Printf("\nVERIFIED  event=%s delivery=%s\n  %s\n", head.Event,
			r.Header.Get(webhook.DeliveryHeader), pretty.String())

		// A fire drill is a synthetic transaction. Acknowledge it so the drill
		// records a pass, and stop here — acting on it would move money against
		// a transaction that never existed. The header is checked rather than
		// the payload so this decision happens before any parsing.
		if r.Header.Get(webhook.DrillHeader) == "true" {
			fmt.Println("  ↳ fire drill: acknowledged, no action taken")
			w.WriteHeader(http.StatusOK)
			return
		}

		// A real handler would now check its own ledger before moving money —
		// the payload is marked advisory precisely because we cannot be trusted
		// as the sole authority.
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	fmt.Printf("reconsync-echo listening on http://%s/hook\n", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
