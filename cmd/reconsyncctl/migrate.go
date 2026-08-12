package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/nobledeveloper01/ReconSync/internal/migrate"
)

func migrateUp(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate up", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	res, err := migrate.Up(ctx, pool)
	if err != nil {
		return err
	}
	for _, v := range res.Applied {
		fmt.Printf("applied %s\n", v)
	}
	if len(res.Applied) == 0 {
		fmt.Printf("schema is up to date (%d migrations)\n", len(res.Skipped))
		return nil
	}
	fmt.Printf("\n%d applied, %d already present\n", len(res.Applied), len(res.Skipped))
	return nil
}

func migrateStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate status", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	applied, pending, err := migrate.Status(ctx, pool)
	if err != nil {
		return err
	}
	for _, v := range applied {
		fmt.Printf("  applied  %s\n", v)
	}
	for _, v := range pending {
		fmt.Printf("  PENDING  %s\n", v)
	}
	if len(pending) > 0 {
		// Non-zero exit so this is usable as a deployment gate: a binary running
		// against a schema it does not expect fails in ways nobody enjoys.
		return fmt.Errorf("%d migrations pending — run: reconsyncctl migrate up", len(pending))
	}
	fmt.Printf("\nschema is up to date (%d migrations)\n", len(applied))
	return nil
}

func migrateBaseline(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate baseline", flag.ContinueOnError)
	confirm := fs.Bool("i-know-the-schema-is-current", false,
		"required: confirms this database already has every migration applied")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// Baselining a database that is actually behind marks migrations as done
	// that never ran, and the damage shows up later as missing columns.
	if !*confirm {
		return errors.New("refusing to baseline without --i-know-the-schema-is-current: " +
			"this marks every migration as applied without running any")
	}

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	marked, err := migrate.Baseline(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("marked %d migrations as applied\n", len(marked))
	return nil
}
