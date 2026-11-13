package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticatedBodyLimitOrgRoster(t *testing.T) {
	cases := []struct {
		method, target string
		want           int64
	}{
		{http.MethodPost, "/v1/org/org-acme/roster", orgRosterBodyLimit},
		{http.MethodGet, "/v1/org/org-acme/roster", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/policy", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org//roster", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/a/b/roster", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/roster?x=1", defaultAuthenticatedBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/card", orgCardBodyLimit},
		{http.MethodGet, "/v1/org/org-acme/card", defaultAuthenticatedBodyLimit},
		{http.MethodPut, "/v1/org/org-acme/join-overrides", orgJoinOverrideBodyLimit},
		{http.MethodPost, "/v1/org/org-acme/join-overrides", defaultAuthenticatedBodyLimit},
		{http.MethodPut, "/v1/org/a/b/join-overrides", defaultAuthenticatedBodyLimit},
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
	if orgCardBodyLimit != 16<<10 || orgJoinOverrideBodyLimit != 16<<10 {
		t.Fatalf("A2 body limits drifted: card=%d override=%d", orgCardBodyLimit, orgJoinOverrideBodyLimit)
	}
}
