package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticatedBodyLimitOrgControlRoutes(t *testing.T) {
	cases := []struct {
		method, target string
		want           int64
	}{
		{http.MethodPost, "/v1/org/org-acme/roster", orgRosterBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/card", orgControlBodyLimit},
		{http.MethodPost, "/v1/federation/join-overrides", orgControlBodyLimit},
		{http.MethodGet, "/v1/org/org-acme/roster", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/policy", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org//roster", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/a/b/roster", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/roster?x=1", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/card?x=1", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/federation/join-overrides?x=1", defaultAuthenticatedBodyLimit},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		if got := authenticatedBodyLimit(r); got != tc.want {
			t.Fatalf("%s %s: limit %d, want %d", tc.method, tc.target, got, tc.want)
		}
	}
	if orgRosterBodyLimit != 2<<20 {
		t.Fatalf("roster body limit drifted: %d", orgRosterBodyLimit)
	}
	if orgControlBodyLimit != 16<<10 {
		t.Fatalf("org control body limit drifted: %d", orgControlBodyLimit)
	}
}
