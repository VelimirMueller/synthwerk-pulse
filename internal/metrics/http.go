package metrics

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
)

// ViewerRoles may read the live status: operators and the admins of any org.
// The status of platform parts holds no org data.
var ViewerRoles = []string{auth.RoleOperator, auth.RoleOwner, auth.RoleAdmin}

// CanViewOrg reports whether p may read the numbers of org: an operator for every
// org, an owner or admin only for the org of the token.
func CanViewOrg(p *auth.Principal, org string) bool {
	if p.IsOperator() {
		return true
	}
	return org != "" && org == p.OrgID && slices.ContainsFunc([]string{auth.RoleOwner, auth.RoleAdmin}, p.HasRole)
}

// ServePlatform serves GET /v1/metrics. Guard it with auth.RequireRole(auth.RoleOperator).
func (c *Collector) ServePlatform(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, c.Platform())
}

// ServeOrg serves GET /v1/orgs/{org}/metrics. It answers 403 unless CanViewOrg.
func (c *Collector) ServeOrg(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.FromContext(r.Context())
	org := r.PathValue("org")
	if !ok || !CanViewOrg(p, org) {
		forbidden(w)
		return
	}
	writeJSON(w, c.Org(org))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// forbidden writes the same problem body as package auth.
func forbidden(w http.ResponseWriter) {
	w.Header().Set("Content-Type", auth.ProblemContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusForbidden)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "about:blank", "title": "Forbidden", "status": http.StatusForbidden,
		"detail": "The token does not allow this request.", "code": auth.CodeForbidden,
	})
}
