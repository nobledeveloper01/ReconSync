package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// FileConfig is one rail's entry in the providers config file.
//
// The secret is named, never written: AuthValueEnv holds the *name* of an
// environment variable, so a config file can be committed and reviewed without
// carrying a live provider key.
type FileConfig struct {
	Name string `json:"name"`

	// Kind selects the adapter: "http" (the default) asks an API, "settlement"
	// reads the files an institution delivers. Most Nigerian banks will not
	// give a fintech a status endpoint but will send a settlement file, so the
	// second is the one that works everywhere.
	Kind string `json:"kind,omitempty"`

	URLTemplate   string   `json:"url_template,omitempty"`
	AuthHeader    string   `json:"auth_header,omitempty"`
	AuthValueEnv  string   `json:"auth_value_env,omitempty"`
	StatusPath    string   `json:"status_path,omitempty"`
	SettledValues []string `json:"settled_values,omitempty"`
	FailedValues  []string `json:"failed_values,omitempty"`
	TimeoutMS     int      `json:"timeout_ms,omitempty"`

	// Settlement configures a kind:"settlement" rail.
	Settlement *SettlementFileConfig `json:"settlement,omitempty"`
}

// SettlementFileConfig describes where an institution's files land and what
// their columns are called, because no two of them agree.
type SettlementFileConfig struct {
	Dir             string   `json:"dir"`
	Glob            string   `json:"glob,omitempty"`
	TimeLayout      string   `json:"time_layout,omitempty"`
	GraceSeconds    int      `json:"grace_seconds,omitempty"`
	SettledStatuses []string `json:"settled_statuses,omitempty"`

	ReferenceColumn string `json:"reference_column,omitempty"`
	AmountColumn    string `json:"amount_column,omitempty"`
	CurrencyColumn  string `json:"currency_column,omitempty"`
	SettledAtColumn string `json:"settled_at_column,omitempty"`
	StatusColumn    string `json:"status_column,omitempty"`
}

// LoadRegistry builds a registry from a JSON config file.
//
// An empty path returns nil, which disables corroboration entirely — the sweep
// then behaves as it did before providers existed.
func LoadRegistry(path string) (*Registry, error) {
	if path == "" {
		return nil, nil
	}

	// The path is deployment configuration supplied by the operator running the
	// binary, not user input — the same trust level as the database URL.
	raw, err := os.ReadFile(filepath.Clean(path)) //nolint:gosec // operator-supplied config path
	if err != nil {
		return nil, fmt.Errorf("provider: read config %s: %w", path, err)
	}

	var entries []FileConfig
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("provider: parse config %s: %w", path, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("provider: config %s registers no rails", path)
	}

	reg := NewRegistry()
	for i, e := range entries {
		if e.Kind == "settlement" {
			p, err := settlementFrom(e)
			if err != nil {
				return nil, fmt.Errorf("provider: entry %d (%s): %w", i, e.Name, err)
			}
			if err := reg.Register(p); err != nil {
				return nil, err
			}
			continue
		}
		if e.Kind != "" && e.Kind != "http" {
			return nil, fmt.Errorf("provider: entry %d (%s): unknown kind %q, want http or settlement",
				i, e.Name, e.Kind)
		}

		authValue := ""
		if e.AuthValueEnv != "" {
			authValue = os.Getenv(e.AuthValueEnv)
			// Starting with an empty credential would make every query fail,
			// and every failure is Unknown, which silently stops all reversals.
			// Better to refuse to start.
			if authValue == "" {
				return nil, fmt.Errorf("provider: %s needs %s, which is unset", e.Name, e.AuthValueEnv)
			}
		}

		p, err := NewHTTP(HTTPConfig{
			ProviderName:  e.Name,
			URLTemplate:   e.URLTemplate,
			AuthHeader:    e.AuthHeader,
			AuthValue:     authValue,
			StatusPath:    e.StatusPath,
			SettledValues: e.SettledValues,
			FailedValues:  e.FailedValues,
			Timeout:       msToDuration(e.TimeoutMS),
		})
		if err != nil {
			return nil, fmt.Errorf("provider: entry %d (%s): %w", i, e.Name, err)
		}
		if err := reg.Register(p); err != nil {
			return nil, err
		}
	}
	return reg, nil
}

func settlementFrom(e FileConfig) (*SettlementProvider, error) {
	if e.Settlement == nil {
		return nil, errors.New(`a settlement rail needs a "settlement" block`)
	}
	c := e.Settlement
	return NewSettlementProvider(SettlementOptions{
		Name:            e.Name,
		Dir:             c.Dir,
		Glob:            c.Glob,
		Layout:          c.TimeLayout,
		SettledStatuses: c.SettledStatuses,
		Grace:           msToDuration(c.GraceSeconds * 1000),
		Columns: SettlementColumns{
			Reference: c.ReferenceColumn,
			Amount:    c.AmountColumn,
			Currency:  c.CurrencyColumn,
			SettledAt: c.SettledAtColumn,
			Status:    c.StatusColumn,
		},
	})
}
