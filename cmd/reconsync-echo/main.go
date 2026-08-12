// Command reconsync-echo is a reference webhook receiver.
//
// It does the one thing every integration must get right: verify the signature
// before trusting the payload. Run it to see deliveries arrive, or read it as
// the worked example of what your own handler needs to do.
package main

import (
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

		var env webhook.Envelope
		if err := json.Unmarshal(body, &env); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		pretty, _ := json.MarshalIndent(env, "  ", "  ")
		fmt.Printf("\nVERIFIED  event=%s delivery=%s\n  %s\n", env.Event,
			r.Header.Get(webhook.DeliveryHeader), pretty)

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
