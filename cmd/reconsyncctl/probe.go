package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"
)

// probe reports whether an HTTP endpoint is healthy, by exit code.
//
// It exists because the runtime image is distroless: no shell, no curl, nothing
// a container health check could otherwise call. Without this the only probe
// available is "the process is running", which stays green through a wedged
// server — exactly the failure a health check is for.
func probe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080/readyz", "endpoint to check")
	timeout := fs.Duration("timeout", 3*time.Second, "how long to wait")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
	if err != nil {
		return fmt.Errorf("bad url %q: %w", *url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s is unreachable: %w", *url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %d", *url, resp.StatusCode)
	}
	fmt.Printf("✓ %s is healthy\n", *url)
	return nil
}
