package webhook

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"syscall"
	"time"
)

var (
	// ErrInsecureScheme means the endpoint was not HTTPS.
	ErrInsecureScheme = errors.New("webhook: endpoint must use https")

	// ErrPrivateAddress means the endpoint resolved inside the private network.
	ErrPrivateAddress = errors.New("webhook: endpoint resolves to a non-public address")
)

// ValidateEndpointURL checks an endpoint at registration time.
//
// This is only the first half of the defence. A host that passes here can be
// re-pointed at an internal address later, so the dial-time check in
// NewTransport is what actually stops DNS rebinding (§10).
func ValidateEndpointURL(raw string, allowPrivate bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("webhook: invalid endpoint url: %w", err)
	}
	if u.Scheme != "https" {
		return ErrInsecureScheme
	}
	if u.Host == "" {
		return fmt.Errorf("webhook: endpoint url has no host")
	}
	if allowPrivate {
		return nil
	}

	// A literal private IP is rejected outright; hostnames are checked again at
	// dial time, when we know what they actually resolve to.
	host := u.Hostname()
	if addr, err := netip.ParseAddr(host); err == nil && !isPublic(addr) {
		return ErrPrivateAddress
	}
	return nil
}

// isPublic reports whether an address is routable on the public internet.
func isPublic(addr netip.Addr) bool {
	if addr.Is4In6() {
		addr = addr.Unmap()
	}

	switch {
	case !addr.IsValid(),
		addr.IsUnspecified(), // 0.0.0.0, ::
		addr.IsLoopback(),    // 127.0.0.0/8, ::1
		addr.IsPrivate(),     // RFC1918, fc00::/7
		addr.IsLinkLocalUnicast(),
		addr.IsLinkLocalMulticast(),
		addr.IsMulticast(),
		addr.IsInterfaceLocalMulticast():
		return false
	}

	// Carrier-grade NAT: not covered by IsPrivate, still not the public internet.
	if addr.Is4() && netip.MustParsePrefix("100.64.0.0/10").Contains(addr) {
		return false
	}
	// IPv4 broadcast, and the cloud metadata address that makes SSRF valuable.
	if addr.Is4() {
		if addr == netip.MustParseAddr("255.255.255.255") || addr == netip.MustParseAddr("169.254.169.254") {
			return false
		}
	}
	return true
}

// TransportOptions configures the outbound client.
type TransportOptions struct {
	// AllowPrivateAddresses disables the dial-time guard. Test-only: enabling it
	// in production turns every webhook endpoint into an SSRF primitive.
	AllowPrivateAddresses bool

	DialTimeout    time.Duration
	RequestTimeout time.Duration
}

// NewClient returns an HTTP client that refuses to reach private addresses.
//
// The check runs in Control, after DNS resolution and immediately before the
// socket connects, so a hostname that resolves to a public address at
// registration and a private one at delivery is still rejected. Validating only
// at registration does not defeat rebinding.
func NewClient(opts TransportOptions) *http.Client {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 10 * time.Second
	}

	dialer := &net.Dialer{
		Timeout:   opts.DialTimeout,
		KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			if opts.AllowPrivateAddresses {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("webhook: unparseable dial address %q: %w", address, err)
			}
			addr, err := netip.ParseAddr(host)
			if err != nil {
				return fmt.Errorf("webhook: unparseable dial address %q: %w", host, err)
			}
			if !isPublic(addr) {
				return ErrPrivateAddress
			}
			return nil
		},
	}

	return &http.Client{
		Timeout: opts.RequestTimeout,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   opts.DialTimeout,
			ResponseHeaderTimeout: opts.RequestTimeout,
			MaxIdleConnsPerHost:   4,
			ForceAttemptHTTP2:     true,
		},
		// Never follow redirects: a 302 to an internal address would sidestep
		// the checks above and re-open the SSRF hole they exist to close.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
