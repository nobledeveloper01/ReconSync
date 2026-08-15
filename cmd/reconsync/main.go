// Command reconsync runs the ingest API, the detection sweep and the webhook
// dispatcher in one process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/correlate"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/drill"
	"github.com/nobledeveloper01/ReconSync/internal/health"
	"github.com/nobledeveloper01/ReconSync/internal/ingest"
	"github.com/nobledeveloper01/ReconSync/internal/licence"
	"github.com/nobledeveloper01/ReconSync/internal/metrics"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/provider"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/secret"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
	webembed "github.com/nobledeveloper01/ReconSync/web/embed"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

type config struct {
	addr        string
	databaseURL string

	// tenantSalt pseudonymises customer references (§8.4). In production this
	// comes from KMS; here it is injected so it never lands in source.
	tenantSalt string

	// webhookSecret signs outbound deliveries. Same story: KMS in production.
	webhookSecret string

	drainTimeout time.Duration

	// allowPrivateWebhookTargets lets webhooks reach private addresses. Off by
	// default; only ever for local development against a loopback receiver.
	allowPrivateWebhookTargets bool

	// providersFile configures rail status adapters. Empty disables
	// corroboration entirely.
	providersFile string

	// licenceToken and licencePublicKey gate the commercial artefacts. Both
	// empty means unlicensed, which serves everything.
	licenceToken     string
	licencePublicKey string

	// checkpointKey signs audit chain heads. Empty disables checkpointing, and
	// the verify endpoint then says so rather than implying a guarantee that
	// is not there.
	checkpointKey string

	// checkpointInterval is the size of the window an attacker can rewrite
	// undetected, so it is configurable rather than fixed.
	checkpointInterval time.Duration

	// minReversalConfidence is the floor below which an orphan is raised for
	// investigation rather than advised as a reversal. Zero keeps every verdict.
	minReversalConfidence float64

	// reportsPerMinute and drillsPerHour bound one tenant's share of a shared
	// deployment. Negative turns a limit off; zero means the default.
	reportsPerMinute float64
	drillsPerHour    float64

	// reversalDeadline is the regulatory clock the sla.at_risk warning and the
	// compliance report both measure against. slaWarnBefore is how much notice
	// the warning gives; negative disables it.
	reversalDeadline time.Duration
	slaWarnBefore    time.Duration
}

func loadConfig() (config, error) {
	c := config{
		addr:          envOr("RECONSYNC_ADDR", ":8080"),
		databaseURL:   os.Getenv("RECONSYNC_DATABASE_URL"),
		tenantSalt:    os.Getenv("RECONSYNC_TENANT_SALT"),
		webhookSecret: os.Getenv("RECONSYNC_WEBHOOK_SECRET"),
		providersFile: os.Getenv("RECONSYNC_PROVIDERS_FILE"),
		checkpointKey: os.Getenv("RECONSYNC_CHECKPOINT_KEY"),
		licenceToken:  os.Getenv("RECONSYNC_LICENCE"),

		licencePublicKey: os.Getenv("RECONSYNC_LICENCE_PUBLIC_KEY"),
		drainTimeout:     20 * time.Second,
	}
	if c.databaseURL == "" {
		return c, errors.New("RECONSYNC_DATABASE_URL is required")
	}
	// Refusing to start beats starting with a predictable salt and silently
	// pseudonymising every customer reference the same way.
	if c.tenantSalt == "" {
		return c, errors.New("RECONSYNC_TENANT_SALT is required")
	}
	if c.webhookSecret == "" {
		return c, errors.New("RECONSYNC_WEBHOOK_SECRET is required")
	}
	if raw := os.Getenv("RECONSYNC_MIN_REVERSAL_CONFIDENCE"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v < 0 || v > 1 {
			return c, fmt.Errorf("RECONSYNC_MIN_REVERSAL_CONFIDENCE must be between 0 and 1, got %q", raw)
		}
		c.minReversalConfidence = v
	}
	if raw := os.Getenv("RECONSYNC_REVERSAL_DEADLINE_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil || secs <= 0 {
			return c, fmt.Errorf("RECONSYNC_REVERSAL_DEADLINE_SECONDS must be a positive integer, got %q", raw)
		}
		c.reversalDeadline = time.Duration(secs) * time.Second
	}
	for _, limit := range []struct {
		name  string
		value *float64
	}{
		{"RECONSYNC_REPORTS_PER_MINUTE", &c.reportsPerMinute},
		{"RECONSYNC_DRILLS_PER_HOUR", &c.drillsPerHour},
	} {
		raw := os.Getenv(limit.name)
		if raw == "" {
			continue
		}
		n, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return c, fmt.Errorf("%s must be a number, got %q", limit.name, raw)
		}
		// Zero would be indistinguishable from unset, and a limit of nothing is
		// not a limit anyone means. Negative is how it is turned off.
		if n == 0 {
			return c, fmt.Errorf("%s must be positive, or negative to disable the limit", limit.name)
		}
		*limit.value = n
	}

	if raw := os.Getenv("RECONSYNC_SLA_WARN_BEFORE_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return c, fmt.Errorf("RECONSYNC_SLA_WARN_BEFORE_SECONDS must be an integer, got %q", raw)
		}
		// Negative disables the warning; zero would be indistinguishable from
		// unset, so it is rejected rather than silently defaulted.
		if secs == 0 {
			return c, errors.New("RECONSYNC_SLA_WARN_BEFORE_SECONDS must be positive, or negative to disable")
		}
		c.slaWarnBefore = time.Duration(secs) * time.Second
	}
	if raw := os.Getenv("RECONSYNC_CHECKPOINT_INTERVAL_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil || secs <= 0 {
			return c, fmt.Errorf("RECONSYNC_CHECKPOINT_INTERVAL_SECONDS must be a positive integer, got %q", raw)
		}
		c.checkpointInterval = time.Duration(secs) * time.Second
	}
	c.allowPrivateWebhookTargets = os.Getenv("RECONSYNC_ALLOW_PRIVATE_WEBHOOK_TARGETS") == "true"
	if raw := os.Getenv("RECONSYNC_DRAIN_TIMEOUT_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil || secs <= 0 {
			return c, fmt.Errorf("RECONSYNC_DRAIN_TIMEOUT_SECONDS must be a positive integer, got %q", raw)
		}
		c.drainTimeout = time.Duration(secs) * time.Second
	}
	return c, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// SIGTERM cancels this, which unwinds every loop below in order.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	db := store.NewPostgres(pool)

	// Rules are read per tenant on the ingest path, so they are cached briefly
	// rather than queried per debit.
	ruleCache, err := rules.NewProvider(db.ListRules, rules.ProviderOptions{})
	if err != nil {
		return err
	}
	ruleProvider := ruleCache.Resolve

	engine, err := correlate.New(db, correlate.Options{
		Rules: ruleProvider,
		Salt: func(_ context.Context, tenantID string) (string, error) {
			return cfg.tenantSalt + ":" + tenantID, nil
		},
	})
	if err != nil {
		return err
	}

	// Records what we dropped, per tenant per minute, so the detection sweep can
	// tell "no credit arrived" apart from "we never saw it" (ADR-0004).
	healthRecorder, err := health.New(db, health.Options{Logger: log})
	if err != nil {
		return err
	}

	pipe, err := pipeline.New(pipeline.HandlerFunc(
		func(ctx context.Context, tenantID string, events []domain.Event) error {
			res, err := engine.Apply(ctx, tenantID, events)
			if err != nil {
				return err
			}
			for _, rej := range res.Rejections {
				log.WarnContext(ctx, "event rejected",
					slog.String("tenant_id", tenantID),
					slog.String("transaction_id", rej.TransactionID),
					slog.String("error", rej.Err.Error()))
			}
			return nil
		}), pipeline.Config{Observer: healthRecorder})
	if err != nil {
		return err
	}
	pipe.Start(ctx)

	// One registry, shared by the loops that report into it and the endpoint
	// that exposes it.
	loopMetrics := metrics.New()

	// A token that will not verify stops startup rather than silently
	// downgrading to unlicensed: a customer who pasted a corrupted key should
	// find out now, not when an auditor asks for a report.
	licenceChecker, err := licence.New(licence.Options{
		Token:     licence.Token(cfg.licenceToken),
		PublicKey: cfg.licencePublicKey,
	})
	if err != nil {
		return err
	}
	if st := licenceChecker.Status(); st.Notice != "" {
		log.Warn("licence", slog.String("customer", st.Customer),
			slog.Int("days_remaining", st.DaysRemaining), slog.String("notice", st.Notice))
	}

	authenticator, err := auth.New(db, auth.Options{})
	if err != nil {
		return err
	}

	if cfg.allowPrivateWebhookTargets {
		log.Warn("webhook SSRF guard disabled: deliveries may reach private addresses. " +
			"This must never be set outside local development.")
	}

	// The drill and the dispatcher share one sender and one secret resolver, so
	// a drill travels exactly the path a real reversal does. Testing a different
	// path would prove nothing about the real one.
	// Each endpoint's own reference, resolved at delivery time. The reference
	// has been stored, carried through the delivery join and passed here since
	// endpoints existed; until now this function ignored it and handed back one
	// global secret, so every tenant on a deployment signed with the same key
	// and rotating one meant rotating all of them at once.
	secrets := secret.New(cfg.webhookSecret).Resolve
	sender := webhook.NewSender(webhook.SenderOptions{
		Client: webhook.NewClient(webhook.TransportOptions{
			AllowPrivateAddresses: cfg.allowPrivateWebhookTargets,
		}),
	})

	drills, err := drill.New(drill.Options{Store: db, Sender: sender, Secrets: secrets})
	if err != nil {
		return err
	}

	api, err := ingest.New(ingest.Options{
		Sink:     pipe,
		Rules:    ruleProvider,
		Store:    db,
		Audit:    db,
		Reports:  db,
		Drills:   drills,
		Claims:   db,
		Webhooks: db,
		Metrics:  loopMetrics,
		Licence:  licenceChecker,
		// Served from this same origin, so the dashboard needs no CORS and the
		// key never crosses an origin boundary.
		Dashboard: webembed.FS(),
		Auth:      authenticator,
		Accounts:  account.NewService(db, time.Now),
		// Per-tenant, so one tenant's runaway loop cannot starve the others.
		// Ingest is deliberately not limited: a debit that is refused is never
		// observed, and a transaction never observed is one whose failure can
		// never be detected.
		ReportsPerMinute: cfg.reportsPerMinute,
		DrillsPerHour:    cfg.drillsPerHour,
		Logger:           log,
		Ready:            func(ctx context.Context) error { return pool.Ping(ctx) },
	})
	if err != nil {
		return err
	}

	// Opt-in. With no config file the sweep behaves exactly as before; with one,
	// every orphan is checked against the rail before a reversal is queued.
	providers, err := provider.LoadRegistry(cfg.providersFile)
	if err != nil {
		return err
	}
	if providers != nil {
		log.Info("provider corroboration enabled", slog.Any("rails", providers.Names()))
	}

	detector, err := service.NewDetector(db, service.DetectorOptions{
		Logger:    log,
		Providers: providers,
		Metrics:   loopMetrics,

		MinReversalConfidence: cfg.minReversalConfidence,

		ReversalDeadline: cfg.reversalDeadline,
		SLAWarnBefore:    cfg.slaWarnBefore,
	})
	if err != nil {
		return err
	}
	dispatcher, err := service.NewDispatcher(db, service.DispatcherOptions{
		Logger:  log,
		Secrets: secrets,
		Sender:  sender,
		Metrics: loopMetrics,
	})
	if err != nil {
		return err
	}

	// Opt-in. Without a key the chain is still verifiable against itself; with
	// one, a wholesale rewrite becomes detectable too.
	var checkpointer *service.Checkpointer
	if cfg.checkpointKey != "" {
		signer, err := audit.NewSigner(cfg.checkpointKey)
		if err != nil {
			return err
		}
		checkpointer, err = service.NewCheckpointer(db, service.CheckpointerOptions{
			Logger: log, Signer: signer, Interval: cfg.checkpointInterval,
		})
		if err != nil {
			return err
		}
		log.Info("audit checkpoints enabled; publish this key so anyone can verify them",
			slog.String("public_key", signer.PublicKey()))
	} else {
		log.Warn("RECONSYNC_CHECKPOINT_KEY is not set: the audit chain is verifiable against itself, " +
			"but a rewrite of the whole chain would not be detectable")
	}

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           api,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); detector.Run(ctx) }()
	go func() { defer wg.Done(); dispatcher.Run(ctx) }()
	go func() { defer wg.Done(); healthRecorder.Run(ctx) }()
	if checkpointer != nil {
		wg.Add(1)
		go func() { defer wg.Done(); checkpointer.Run(ctx) }()
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("listening", slog.String("addr", cfg.addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Graceful shutdown, in dependency order: stop accepting requests, drain the
	// pipeline, then let the background loops finish. WithoutCancel because the
	// signal that triggered shutdown must not cancel the drain it started.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.drainTimeout)
	defer cancel()

	if err := srv.Shutdown(drainCtx); err != nil {
		log.Error("http shutdown", slog.String("error", err.Error()))
	}
	pipe.Close() // flushes buffered events
	wg.Wait()

	log.Info("stopped", slog.Any("pipeline", pipe.Stats()))
	return nil
}
