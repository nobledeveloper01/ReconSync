package provider

import (
	"encoding/json"
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
	Name          string   `json:"name"`
	URLTemplate   string   `json:"url_template"`
	AuthHeader    string   `json:"auth_header,omitempty"`
	AuthValueEnv  string   `json:"auth_value_env,omitempty"`
	StatusPath    string   `json:"status_path"`
	SettledValues []string `json:"settled_values,omitempty"`
	FailedValues  []string `json:"failed_values,omitempty"`
	TimeoutMS     int      `json:"timeout_ms,omitempty"`
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
