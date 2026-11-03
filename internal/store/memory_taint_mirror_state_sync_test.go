package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequirePristineStateSyncProjectionTaintMirrorSeeds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statements []string
		wantError  string
	}{
		{name: "canonical zero seed"},
		{name: "advanced revision", statements: []string{`UPDATE memory_taint_mirror_revision SET revision = 1`}, wantError: "pristine memory taint mirror revision"},
		{name: "missing seed", statements: []string{`DELETE FROM memory_taint_mirror_revision`}, wantError: "pristine memory taint mirror revision"},
		{name: "noncanonical singleton", statements: []string{`PRAGMA ignore_check_constraints = ON`, `UPDATE memory_taint_mirror_revision SET singleton = 2`}, wantError: "pristine memory taint mirror revision"},
		{name: "extra revision row", statements: []string{`PRAGMA ignore_check_constraints = ON`, `INSERT INTO memory_taint_mirror_revision(singleton, revision) VALUES (2, 0)`}, wantError: "pristine memory taint mirror revision"},
		{name: "materialized membership", statements: []string{`INSERT INTO memory_taint_mirror(memory_id) VALUES ('stale-membership')`}, wantError: `table "memory_taint_mirror" is populated`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := newTestStore(t)
			require.NoError(t, s.RequirePristineStateSyncProjection(ctx))
			for _, statement := range tc.statements {
				_, err := s.conn.ExecContext(ctx, statement)
				require.NoError(t, err)
			}
			err := s.RequirePristineStateSyncProjection(ctx)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
		})
	}
}
