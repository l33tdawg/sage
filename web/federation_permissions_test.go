package web

import (
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func TestNormalizeRequestedPeerPermissionsEnforcesDependencies(t *testing.T) {
	permissions, err := normalizeRequestedPeerPermissions([]store.PeerRBACDomainPermission{
		{Domain: "tii.read", Read: true},
		{Domain: "tii.copy", Copy: true},
		{Domain: "tii.none"},
	})
	require.NoError(t, err)
	require.Equal(t, []store.PeerRBACDomainPermission{
		{Domain: "tii.copy", Read: true, Copy: true},
		{Domain: "tii.read", Read: true},
	}, permissions)
}

func TestNormalizeRequestedPeerPermissionsRejectsWriteCapability(t *testing.T) {
	permissions, err := normalizeRequestedPeerPermissions([]store.PeerRBACDomainPermission{
		{Domain: "tii", Write: true},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "consensus-bound ingress capability")
	require.Nil(t, permissions)
}

func TestClonePeerPermissionsSanitizesStaleWriteAndCanonicalizesCopy(t *testing.T) {
	permissions := clonePeerPermissions([]store.PeerRBACDomainPermission{{
		Domain: "tii", Write: true, Copy: true,
	}})
	require.Equal(t, []store.PeerRBACDomainPermission{{
		Domain: "tii", Read: true, Write: false, Copy: true,
	}}, permissions)
}
