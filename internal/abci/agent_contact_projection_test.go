package abci

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectionWritesAgentContactsIsNarrow(t *testing.T) {
	require.True(t, projectionWritesAgentContacts([]pendingWrite{{writeType: "agent_register"}}))
	require.True(t, projectionWritesAgentContacts([]pendingWrite{{writeType: "agent_update"}}))
	require.True(t, projectionWritesAgentContacts([]pendingWrite{{writeType: "agent_permission"}}))
	require.False(t, projectionWritesAgentContacts([]pendingWrite{
		{writeType: "memory"}, {writeType: "status_update"}, {writeType: "federation"},
	}))
}
