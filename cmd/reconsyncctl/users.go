package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// User management from the server.
//
// This exists for the case the dashboard cannot cover: the only admin has lost
// their authenticator, or a tenant has no users at all. Anyone with a shell on
// the box already has the database, so the CLI is not a privilege boundary —
// it is the way back in when the browser path is closed.

func usersService(ctx context.Context) (*account.Service, func(), error) {
	pool, err := connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	return account.NewService(store.NewPostgres(pool), time.Now), pool.Close, nil
}

func usersCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("users create", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	email := fs.String("email", "", "email address")
	role := fs.String("role", "viewer", "viewer, operator or admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" || *email == "" {
		return errors.New("--tenant and --email are required")
	}

	password, err := readPassword("password (at least 12 characters): ")
	if err != nil {
		return err
	}
	confirm, err := readPassword("again: ")
	if err != nil {
		return err
	}
	if password != confirm {
		return errors.New("those did not match")
	}

	svc, closeFn, err := usersService(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	u, err := svc.Create(ctx, *tenant, *email, password, account.Role(*role))
	if err != nil {
		return err
	}

	fmt.Printf("user id: %s\nemail:   %s\nrole:    %s\n", u.ID, u.Email, u.Role)
	if account.Role(*role) != u.Role {
		fmt.Printf("\nThis is the first account for %s, so it was made an admin —\n"+
			"otherwise nobody could grant the first admin their role.\n", *tenant)
	}
	fmt.Println("\nTwo-factor is not set up yet. Sign in and enrol before this account " +
		"is used for anything real.")
	return nil
}

func usersList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("users list", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("--tenant is required")
	}

	svc, closeFn, err := usersService(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	users, err := svc.Store().ListUsers(ctx, *tenant)
	if err != nil {
		return err
	}
	if len(users) == 0 {
		fmt.Printf("No users for %s. Create one with: reconsyncctl users create --tenant %s --email you@example.com\n",
			*tenant, *tenant)
		return nil
	}

	fmt.Printf("%-24s  %-32s  %-9s  %-4s  %s\n", "ID", "EMAIL", "ROLE", "2FA", "STATUS")
	for _, u := range users {
		status := "active"
		switch {
		case u.DisabledAt != nil:
			status = "disabled"
		case u.LockedUntil != nil && time.Now().Before(*u.LockedUntil):
			// The number matters: an operator deciding whether to intervene
			// needs to know if it clears in a minute or in a quarter of an hour.
			status = fmt.Sprintf("locked for %s", time.Until(*u.LockedUntil).Round(time.Second))
		}
		twoFA := "no"
		if u.TOTPEnabled {
			twoFA = "yes"
		}
		fmt.Printf("%-24s  %-32s  %-9s  %-4s  %s\n", u.ID, u.Email, u.Role, twoFA, status)
	}
	return nil
}

func usersResetPassword(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("users reset-password", flag.ContinueOnError)
	email := fs.String("email", "", "email address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *email == "" {
		return errors.New("--email is required")
	}

	password, err := readPassword("new password (at least 12 characters): ")
	if err != nil {
		return err
	}
	if err := account.ValidatePassword(password); err != nil {
		return err
	}

	svc, closeFn, err := usersService(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	u, err := svc.Store().UserByEmail(ctx, *email)
	if err != nil {
		return err
	}
	hash, err := account.HashPassword(password)
	if err != nil {
		return err
	}
	// SetPassword also clears the lockout and ends every session, which is what
	// makes this the way back in for someone locked out.
	if err := svc.Store().SetPassword(ctx, u.ID, hash); err != nil {
		return err
	}

	fmt.Printf("Password set for %s. Every session was signed out.\n", u.Email)
	if u.TOTPEnabled {
		fmt.Println("\nTwo-factor is still on. If the authenticator is what was lost, " +
			"run: reconsyncctl users disable-2fa --email " + u.Email)
	}
	return nil
}

func usersDisable2FA(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("users disable-2fa", flag.ContinueOnError)
	email := fs.String("email", "", "email address")
	confirm := fs.Bool("yes", false, "confirm; this weakens the account")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *email == "" {
		return errors.New("--email is required")
	}
	if !*confirm {
		return errors.New("pass --yes to confirm: this turns off the second factor " +
			"and leaves the account on a password alone")
	}

	svc, closeFn, err := usersService(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	u, err := svc.Store().UserByEmail(ctx, *email)
	if err != nil {
		return err
	}
	if err := svc.Store().DisableTOTP(ctx, u.ID); err != nil {
		return err
	}
	// Sessions too: whoever asked for this may not be the account holder, and
	// leaving existing sessions alive would hide that.
	if err := svc.Store().DeleteUserSessions(ctx, u.ID); err != nil {
		return err
	}

	fmt.Printf("Two-factor is off for %s and every session ended.\n", u.Email)
	fmt.Println("Tell them to enrol a new authenticator at their next sign-in.")
	return nil
}

func usersDisable(ctx context.Context, args []string) error {
	return setUserDisabled(ctx, args, true)
}

func usersEnable(ctx context.Context, args []string) error {
	return setUserDisabled(ctx, args, false)
}

func setUserDisabled(ctx context.Context, args []string, disabled bool) error {
	name := "users enable"
	if disabled {
		name = "users disable"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	email := fs.String("email", "", "email address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *email == "" {
		return errors.New("--email is required")
	}

	svc, closeFn, err := usersService(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	u, err := svc.Store().UserByEmail(ctx, *email)
	if err != nil {
		return err
	}
	if err := svc.Store().SetUserDisabled(ctx, u.TenantID, u.ID, disabled); err != nil {
		return err
	}

	if disabled {
		fmt.Printf("%s is disabled and every session ended.\n", u.Email)
		return nil
	}
	fmt.Printf("%s is enabled again. They still need their password.\n", u.Email)
	return nil
}

func usersRole(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("users role", flag.ContinueOnError)
	email := fs.String("email", "", "email address")
	role := fs.String("role", "", "viewer, operator or admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *email == "" || *role == "" {
		return errors.New("--email and --role are required")
	}
	if !account.Role(*role).Valid() {
		return fmt.Errorf("--role must be viewer, operator or admin, got %q", *role)
	}

	svc, closeFn, err := usersService(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	u, err := svc.Store().UserByEmail(ctx, *email)
	if err != nil {
		return err
	}
	if err := svc.Store().UpdateUserRole(ctx, u.TenantID, u.ID, account.Role(*role)); err != nil {
		return err
	}

	fmt.Printf("%s is now %s (%s).\n", u.Email, *role,
		strings.Join(account.Role(*role).Scopes(), ", "))
	return nil
}

// stdin is read through one buffered reader for the whole process.
//
// A fresh bufio.Reader per prompt throws away whatever it read ahead, so the
// second of two piped lines vanished and the command failed with EOF — with
// the first line already consumed and no way to tell that from a truncated
// input.
var stdin = bufio.NewReader(os.Stdin)

// readPassword reads without echoing when there is a terminal to read from.
//
// The fallback matters for scripting, and it says so: a password typed into a
// pipe is in the shell history and in the process list, which the operator
// should know before it happens rather than after.
func readPassword(prompt string) (string, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		fmt.Fprintln(os.Stderr, "warning: stdin is not a terminal, so the password will not be hidden.")
		line, err := stdin.ReadString('\n')
		if err != nil && line == "" {
			return "", err
		}
		return strings.TrimRight(line, "\r\n"), nil
	}

	fmt.Fprint(os.Stderr, prompt)
	b, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
