package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// allowPrivateUsage is repeated on both commands that talk to an endpoint.
const allowPrivateUsage = "permit private/loopback addresses (local development only)"

func endpointsCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("endpoints create", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	url := fs.String("url", "", "https endpoint that receives webhooks")
	id := fs.String("id", "", "endpoint id (generated when omitted)")
	events := fs.String("events", "", "comma-separated event types (empty means all)")
	secretRef := fs.String("secret-ref", "env://RECONSYNC_WEBHOOK_SECRET", "reference to the signing secret")
	allowPrivate := fs.Bool("allow-private", false, allowPrivateUsage)
	allowInsecure := fs.Bool("allow-insecure", false,
		"permit an http endpoint. Local development only: plaintext puts every payload on the wire")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}
	if *url == "" {
		return errors.New("--url is required")
	}

	// Rejected here as well as at delivery time. Catching it now means the
	// operator finds out while typing the command, not from a dead-letter queue
	// six hours later.
	var opts []webhook.URLOption
	if *allowInsecure {
		opts = append(opts, webhook.AllowInsecureScheme())
	}
	if err := webhook.ValidateEndpointURL(*url, *allowPrivate, opts...); err != nil {
		if errors.Is(err, webhook.ErrPrivateAddress) && !*allowPrivate {
			return fmt.Errorf("%w — pass --allow-private only for local development", err)
		}
		if errors.Is(err, webhook.ErrInsecureScheme) && !*allowInsecure {
			return fmt.Errorf("%w — pass --allow-insecure only for local development", err)
		}
		return err
	}

	if *id == "" {
		generated, err := generateID("we")
		if err != nil {
			return err
		}
		*id = generated
	}

	var eventList []string
	if *events != "" {
		for _, e := range strings.Split(*events, ",") {
			if trimmed := strings.TrimSpace(e); trimmed != "" {
				eventList = append(eventList, trimmed)
			}
		}
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	ep := &store.WebhookEndpoint{
		ID:        *id,
		TenantID:  *tenant,
		URL:       *url,
		SecretRef: *secretRef,
		Events:    eventList,
		Enabled:   true,
	}
	if err := store.NewPostgres(pool).CreateEndpoint(ctx, *tenant, ep); err != nil {
		return err
	}

	subscribed := "all events"
	if len(eventList) > 0 {
		subscribed = strings.Join(eventList, ", ")
	}
	fmt.Printf("endpoint %s created for %s\n  url:    %s\n  events: %s\n", *id, *tenant, *url, subscribed)
	fmt.Printf("\nVerify it now:  reconsyncctl endpoints test --tenant %s --id %s\n", *tenant, *id)
	return nil
}

func endpointsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("endpoints list", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
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

	eps, err := store.NewPostgres(pool).ListEndpoints(ctx, *tenant)
	if err != nil {
		return err
	}
	if len(eps) == 0 {
		fmt.Printf("no endpoints for %s\n\nCreate one:  reconsyncctl endpoints create --tenant %s --url https://...\n", *tenant, *tenant)
		return nil
	}

	fmt.Printf("%-16s  %-7s  %-40s  %s\n", "ID", "ENABLED", "URL", "EVENTS")
	for _, ep := range eps {
		subscribed := "all"
		if len(ep.Events) > 0 {
			subscribed = strings.Join(ep.Events, ",")
		}
		fmt.Printf("%-16s  %-7t  %-40s  %s\n", ep.ID, ep.Enabled, ep.URL, subscribed)
	}
	return nil
}

// endpointsTest sends a signed ping, so an operator can prove the receiver works
// before a real reversal depends on it. Nobody otherwise exercises this path
// until the first incident, which is the worst time to discover it is broken.
func endpointsTest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("endpoints test", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	id := fs.String("id", "", "endpoint id")
	// No --allow-insecure here: the scheme was decided when the endpoint was
	// registered, and offering a flag that changes nothing would be a lie.
	allowPrivate := fs.Bool("allow-private", false, allowPrivateUsage)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}
	if *id == "" {
		return errors.New("--id is required")
	}

	secret := os.Getenv("RECONSYNC_WEBHOOK_SECRET")
	if secret == "" {
		return errors.New("RECONSYNC_WEBHOOK_SECRET is required to sign the test payload")
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	eps, err := store.NewPostgres(pool).ListEndpoints(ctx, *tenant)
	if err != nil {
		return err
	}
	var target *store.WebhookEndpoint
	for _, ep := range eps {
		if ep.ID == *id {
			target = ep
			break
		}
	}
	if target == nil {
		return fmt.Errorf("no endpoint %q for tenant %q — run: reconsyncctl endpoints list --tenant %s", *id, *tenant, *tenant)
	}

	payload, err := webhook.Marshal(webhook.Envelope{
		Event:      webhook.EventType("endpoint.test"),
		OccurredAt: time.Now().UTC(),
		Data: webhook.Data{
			TransactionID: "TEST-PING",
			AmountMinor:   1,
			Currency:      "NGN",
			Reason:        "connectivity_test",
			Advisory:      true,
		},
	})
	if err != nil {
		return err
	}

	sender := webhook.NewSender(webhook.SenderOptions{
		Client: webhook.NewClient(webhook.TransportOptions{AllowPrivateAddresses: *allowPrivate}),
	})
	res := sender.Send(ctx, webhook.Delivery{
		TenantID:      *tenant,
		EndpointID:    target.ID,
		TransactionID: "TEST-PING",
		URL:           target.URL,
		Secret:        secret,
		Event:         webhook.EventType("endpoint.test"),
		Payload:       payload,
	})

	switch {
	case res.Err != nil:
		return fmt.Errorf("could not reach %s: %w\n  The endpoint must be reachable from this host over https", target.URL, res.Err)
	case res.Delivered:
		fmt.Printf("✓ %s accepted the test payload (HTTP %d in %s)\n", target.URL, res.StatusCode, res.Duration.Round(time.Millisecond))
		fmt.Println("\nThe receiver verified nothing on our side — confirm it checked the")
		fmt.Println("X-ReconSync-Signature header before trusting a real reversal.")
		return nil
	default:
		return fmt.Errorf("%s returned HTTP %d, which is not a success\n  body: %s\n  A reversal webhook would be retried and then dead-lettered",
			target.URL, res.StatusCode, truncate(res.ResponseBody, 200))
	}
}

func generateID(prefix string) (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(b[:]), nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
