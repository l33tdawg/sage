package rest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestTaintProjectionErrorsAreSkippableRecordLocalDenials(t *testing.T) {
	for _, sentinel := range []error{
		store.ErrMemoryProjectionTainted,
		store.ErrMemoryProjectionTaintPending,
	} {
		err := fmt.Errorf("%w: %w", store.ErrMemoryProjectionUnpublished, sentinel)
		require.True(t, isUnsafeAppV23Projection(err))
	}
}
