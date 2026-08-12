package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The CLI is package main, so it can only be exercised the way a user does: by
// running it. Built once for the whole package — a t.TempDir() would be removed
// when its owning test finished, leaving later tests with no binary.
var (
	ctlPath string
	ctlErr  error
)

// os.Exit lives only here, so the cleanup deferred in runTests actually runs.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "reconsync-cli")
	if err != nil {
		ctlErr = err
		return m.Run()
	}
	defer func() { _ = os.RemoveAll(dir) }()

	ctlPath = filepath.Join(dir, "reconsyncctl")
	cmd := exec.CommandContext(context.Background(), "go", "build", "-o", ctlPath, "../cmd/reconsyncctl")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		ctlErr = fmt.Errorf("%w: %s", err, stderr.String())
	}

	return m.Run()
}

func reconsyncctl(t *testing.T) string {
	t.Helper()
	if ctlErr != nil {
		t.Fatalf("could not build reconsyncctl: %v", ctlErr)
	}
	return ctlPath
}

// runCtl invokes the CLI with no database configured, so only argument handling
// is exercised.
func runCtl(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()

	cmd := exec.CommandContext(context.Background(), reconsyncctl(t), args...)
	cmd.Env = []string{"PATH=" + pathEnv()}

	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb

	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run %v: %v", args, err)
		}
		code = exitErr.ExitCode()
	}
	return out.String(), errb.String(), code
}

func pathEnv() string {
	// Keep go's toolchain reachable for the build, nothing else needed.
	return "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin"
}

// A valid noun with no verb must name the missing verb, not claim the noun is
// unknown.
func TestCLINounWithoutVerb(t *testing.T) {
	for _, noun := range []string{"tenant", "keys"} {
		stdout, stderr, code := runCtl(t, noun)

		if code == 0 {
			t.Errorf("%s with no verb exited 0, want non-zero", noun)
		}
		if !strings.Contains(stderr, "needs a subcommand") {
			t.Errorf("%s: stderr = %q, want it to name the missing subcommand", noun, stderr)
		}
		if strings.Contains(stderr, "unknown command") {
			t.Errorf("%s: reported the noun as unknown, but it is valid", noun)
		}
		if stdout != "" {
			t.Errorf("%s: wrote %q to stdout on failure, want stderr only", noun, stdout)
		}
	}
}

func TestCLIUnknownVerbNamesTheValidOnes(t *testing.T) {
	_, stderr, code := runCtl(t, "keys", "delete")

	if code == 0 {
		t.Error("unknown verb exited 0, want non-zero")
	}
	if !strings.Contains(stderr, "create") {
		t.Errorf("stderr = %q, want it to list the valid verbs", stderr)
	}
}

// Failure output belongs on stderr, so a caller piping stdout sees nothing.
func TestCLIUsageGoesToStderrOnFailure(t *testing.T) {
	stdout, stderr, code := runCtl(t, "bogus")

	if code == 0 {
		t.Error("unknown command exited 0, want non-zero")
	}
	if stdout != "" {
		t.Errorf("stdout = %q on failure, want empty", stdout)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr = %q, want the usage text", stderr)
	}
	if !strings.Contains(stderr, `unknown command "bogus"`) {
		t.Errorf("stderr = %q, want it to name the unknown command", stderr)
	}
}

// help is a success path, so it goes to stdout and exits 0.
func TestCLIHelpGoesToStdout(t *testing.T) {
	for _, flag := range []string{"help", "-h", "--help"} {
		stdout, _, code := runCtl(t, flag)

		if code != 0 {
			t.Errorf("%s exited %d, want 0", flag, code)
		}
		if !strings.Contains(stdout, "Usage:") {
			t.Errorf("%s: stdout = %q, want the usage text", flag, stdout)
		}
	}
}

func TestCLINoArgs(t *testing.T) {
	stdout, stderr, code := runCtl(t)

	if code == 0 {
		t.Error("no args exited 0, want non-zero")
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "no command given") {
		t.Errorf("stderr = %q, want it to say no command was given", stderr)
	}
}

// Endpoint validation runs before the database connection, so a bad URL is
// reported while the operator is still typing rather than from a dead-letter
// queue six hours later.
func TestCLIEndpointsRejectsBadURLs(t *testing.T) {
	cases := []struct {
		name, url, want string
	}{
		{"plaintext http", "http://customer.example.com/hook", "must use https"},
		{"loopback", "https://127.0.0.1/hook", "non-public address"},
		{"private range", "https://10.0.0.5/hook", "non-public address"},
		{"cloud metadata", "https://169.254.169.254/latest", "non-public address"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, code := runCtl(t, "endpoints", "create", "--tenant", "tnt_x", "--url", tc.url)

			if code == 0 {
				t.Fatalf("accepted %s", tc.url)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr = %q, want it to mention %q", stderr, tc.want)
			}
		})
	}

	// The private-address refusal must name the escape hatch.
	_, stderr, _ := runCtl(t, "endpoints", "create", "--tenant", "tnt_x", "--url", "https://127.0.0.1/hook")
	if !strings.Contains(stderr, "--allow-private") {
		t.Errorf("stderr = %q, want it to name the --allow-private flag", stderr)
	}
}

func TestCLIEndpointsRequiresArguments(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"endpoints", "create", "--url", "https://x.example.com/h"}, "--tenant is required"},
		{[]string{"endpoints", "create", "--tenant", "tnt_x"}, "--url is required"},
		{[]string{"endpoints", "list"}, "--tenant is required"},
		{[]string{"endpoints", "test", "--id", "we_1"}, "--tenant is required"},
		{[]string{"endpoints", "test", "--tenant", "tnt_x"}, "--id is required"},
	}
	for _, tc := range cases {
		_, stderr, code := runCtl(t, tc.args...)

		if code == 0 {
			t.Errorf("%v exited 0", tc.args)
		}
		if !strings.Contains(stderr, tc.want) {
			t.Errorf("%v: stderr = %q, want %q", tc.args, stderr, tc.want)
		}
	}
}

// Signing a test payload needs the same secret the server signs with; say so
// rather than sending an unverifiable payload.
func TestCLIEndpointsTestRequiresSigningSecret(t *testing.T) {
	_, stderr, code := runCtl(t, "endpoints", "test", "--tenant", "tnt_x", "--id", "we_1")

	if code == 0 {
		t.Fatal("exited 0 with no signing secret configured")
	}
	if !strings.Contains(stderr, "RECONSYNC_WEBHOOK_SECRET") {
		t.Errorf("stderr = %q, want it to name the missing secret", stderr)
	}
}

// Every command needing a database must say so, rather than failing obscurely.
func TestCLIReportsMissingDatabaseURL(t *testing.T) {
	for _, args := range [][]string{
		{"doctor"},
		{"tenant", "create", "--id", "tnt_x"},
		{"keys", "create", "--tenant", "tnt_x"},
	} {
		_, stderr, code := runCtl(t, args...)

		if code == 0 {
			t.Errorf("%v exited 0 with no database configured", args)
		}
		if !strings.Contains(stderr, "RECONSYNC_DATABASE_URL") {
			t.Errorf("%v: stderr = %q, want it to name the missing variable", args, stderr)
		}
	}
}
