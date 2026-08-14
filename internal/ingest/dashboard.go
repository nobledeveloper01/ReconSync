package ingest

import (
	"io/fs"
	"net/http"
	"strings"
)

// The dashboard is served by this binary, from the same origin as the API.
//
// That is the whole reason it can exist safely. A dashboard hosted elsewhere
// would need CORS opened on an API that advises money movement, and would put
// the customer's key through a cross-origin request. Same origin needs neither,
// and it ships as one binary with nothing else to deploy.

// DashboardFS is the built assets, supplied by the caller so this package does
// not decide where they come from — the server binary embeds them, and a build
// without them simply has no dashboard.
type DashboardFS = fs.FS

// mountDashboard serves the single-page app at the root.
func (s *Server) mountDashboard(mux *http.ServeMux) {
	if s.dashboard == nil {
		return
	}

	files := http.FileServer(http.FS(s.dashboard))

	// Registered without a method: "GET /" is more general in path than "/v1/"
	// but narrower in method, which ServeMux rejects as ambiguous. Matching all
	// methods here and refusing the wrong one below is the unambiguous form.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Anything that is not a real file is the app itself: the router lives
		// in the fragment, but a refresh on a deep link must still load it
		// rather than 404.
		clean := strings.TrimPrefix(r.URL.Path, "/")
		if clean != "" {
			if f, err := s.dashboard.Open(clean); err == nil {
				_ = f.Close()
				setDashboardHeaders(w, clean)
				files.ServeHTTP(w, r)
				return
			}
		}

		index, err := fs.ReadFile(s.dashboard, "index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		setDashboardHeaders(w, "index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
}

// setDashboardHeaders locks the page down to what it actually needs.
func setDashboardHeaders(w http.ResponseWriter, name string) {
	// The dashboard holds an API key in the tab. A content security policy that
	// forbids everything except this origin means an injected script has
	// nowhere to send it, and no third party can inject one in the first place.
	w.Header().Set("Content-Security-Policy",
		"default-src 'self'; connect-src 'self'; img-src 'self' data:; "+
			"style-src 'self'; script-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")

	// The hashed assets never change; index.html must not be cached or a deploy
	// leaves browsers on the old app pointing at a new API.
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
}
