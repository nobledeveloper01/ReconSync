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

	"github.com/nobledeveloper01/ReconSync/internal/secret"
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
	secretRef := fs.String("secret-ref", store.DefaultSecretRef, "reference to the signing secret")
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

	// Resolved from the endpoint's own reference, not from the global variable.
	// Signing the test with a different secret to the one real deliveries use
	// would make this command pass for an endpoint that is about to reject
	// every reversal — which is the opposite of what it is for.
	secrets, err := secret.New(os.Getenv("RECONSYNC_WEBHOOK_SECRET")).Resolve(ctx, target.SecretRef)
	if err != nil {
		return fmt.Errorf("%w\n  The signing secret must be resolvable from this host, exactly as the server resolves it", err)
	}

	sender := webhook.NewSender(webhook.SenderOptions{
		Client: webhook.NewClient(webhook.TransportOptions{AllowPrivateAddresses: *allowPrivate}),
	})
	res := sender.Send(ctx, webhook.Delivery{
		TenantID:      *tenant,
		EndpointID:    target.ID,
		TransactionID: "TEST-PING",
		URL:           target.URL,
		Secrets:       secrets,
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

// endpointsRotate points an endpoint at a new signing secret.
//
// Rotation is two commands, not one, because the safe order matters. Starting
// it signs every payload with the new secret and the old one at once, so a
// receiver holding either keeps working and neither side has to change at the
// same moment. Finishing it drops the old.
func endpointsRotate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("endpoints rotate", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	id := fs.String("id", "", "endpoint id")
	to := fs.String("to", "", "reference to the new secret, e.g. env://ACME_WEBHOOK_SECRET_2026")
	finish := fs.Bool("finish", false, "drop the old secret, leaving only --to")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case *tenant == "":
		return errors.New("--tenant is required")
	case *id == "":
		return errors.New("--id is required")
	case *to == "":
		return errors.New("--to is required, e.g. --to env://ACME_WEBHOOK_SECRET_2026")
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	db := store.NewPostgres(pool)
	eps, err := db.ListEndpoints(ctx, *tenant)
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
		return fmt.Errorf("no endpoint %q for tenant %q — run: reconsyncctl endpoints list --tenant %s",
			*id, *tenant, *tenant)
	}

	ref := *to
	if !*finish {
		// The old reference is kept alongside the new one. Every payload then
		// carries a signature for each, which is what lets the receiver change
		// over on a different day to the sender.
		previous := target.SecretRef
		if previous == "" {
			previous = store.DefaultSecretRef
		}
		if previous != *to {
			ref = *to + "," + previous
		}
	}

	// Resolved before it is stored. An unresolvable reference written to the
	// database would stop every delivery to this endpoint, and the operator
	// would find out from a dead-letter queue rather than from this command.
	resolver := secret.New(os.Getenv("RECONSYNC_WEBHOOK_SECRET"))
	if _, err := resolver.Resolve(ctx, ref); err != nil {
		return fmt.Errorf("%w\n  Set it on this host and on the server before rotating", err)
	}

	if err := db.SetEndpointSecretRef(ctx, *tenant, *id, ref); err != nil {
		return err
	}

	if *finish {
		fmt.Printf("%s now signs with %s only.\n", *id, *to)
		fmt.Println("\nThe old secret no longer verifies. Remove it from the receiver.")
		return nil
	}

	fmt.Printf("%s now signs with both:\n  new  %s\n  old  %s\n", *id, *to, target.SecretRef)
	fmt.Println("\nEvery payload now carries a signature for each, so the receiver keeps")
	fmt.Println("working whether it holds the old secret or the new one.")
	fmt.Printf("\nOnce the receiver has the new secret, finish:\n"+
		"  reconsyncctl endpoints rotate --tenant %s --id %s --to %s --finish\n", *tenant, *id, *to)
	return nil
}
