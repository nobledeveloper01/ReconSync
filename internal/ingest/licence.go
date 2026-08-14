package ingest

import (
	"net/http"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
)

// The one place licensing is enforced.
//
// Four endpoints hang off this, and nothing else in the codebase asks about a
// licence. That is deliberate: the review that shaped this said scattering
// checks adds bugs rather than security, and what is being protected is a
// report, not a payment.
//
// Detection, ingest and reversal delivery are conspicuously absent. An expired
// licence must never stop the thing that keeps a customer's money safe — an
// expiry that generates incidents is a liability, not a commercial control.

// requireLicence reports whether the commercial artefacts may be served.
func (s *Server) requireLicence(w http.ResponseWriter, r *http.Request) bool {
	if s.licence == nil || s.licence.ArtefactsAvailable() {
		return true
	}

	// 402, not 403: this is not an authorisation failure, and calling it one
	// would send an operator hunting for a permissions bug.
	s.writeError(w, r, http.StatusPaymentRequired, "licence_expired",
		s.licence.Status().Notice, "")
	return false
}

// handleLicence reports where the licence stands, including the countdown.
//
// Always available, expired or not. A customer whose reports have stopped needs
// this endpoint most, and withholding the explanation along with the artefact
// would be the worst possible time to go quiet.
func (s *Server) handleLicence(w http.ResponseWriter, r *http.Request) {
	if _, ok := auth.PrincipalFrom(r.Context()); !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}
	if s.licence == nil {
		s.writeJSON(w, r, http.StatusOK, map[string]any{
			"licensed": false,
			"notice":   "no licence is configured; every feature is available",
		})
		return
	}
	s.writeJSON(w, r, http.StatusOK, s.licence.Status())
}
