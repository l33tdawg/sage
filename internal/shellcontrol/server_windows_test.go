//go:build windows

package shellcontrol

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowsNamedPipeStatusAndClose(t *testing.T) {
	server, err := Start(t.TempDir(), "11.11.0-test", "http://127.0.0.1:8080")
	require.NoError(t, err)

	timeout := time.Second
	conn, err := winio.DialPipe(server.Endpoint(), &timeout)
	require.NoError(t, err)
	payload, err := json.Marshal(Request{ControlProtocol: 1, ShellProtocol: 1, Operation: "status"})
	require.NoError(t, err)
	require.NoError(t, writeFrame(conn, payload))
	responsePayload, err := readFrame(conn)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	var response Response
	require.NoError(t, json.Unmarshal(responsePayload, &response))
	assert.Equal(t, StateStarting, response.State)
	assert.Empty(t, response.UIOrigin)

	require.NoError(t, server.SetState(StateReady))
	require.NoError(t, server.Close())
	_, err = winio.DialPipe(server.Endpoint(), &timeout)
	assert.Error(t, err, "closed named pipe must not accept a second shell")
}

func TestWindowsPipeIdentityIsProfileScoped(t *testing.T) {
	first, err := windowsHomeIdentity(`C:\Users\SAGE\.sage\`)
	require.NoError(t, err)
	second, err := windowsHomeIdentity(`C:\Users\SAGE\fixture\..\.sage`)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, "e4ec5178983b20c1", windowsPipeSuffix("S-1-5-21-123", first),
		"Go and Rust must retain the published cross-language pipe vector")
	assert.Equal(t, windowsPipeSuffix("S-1-5-21-123", first), windowsPipeSuffix("S-1-5-21-123", second))
	assert.NotEqual(t, windowsPipeSuffix("S-1-5-21-123", first), windowsPipeSuffix("S-1-5-21-123", `d:\fixture\sage-home`))
	assert.Equal(t, `\\server\share\.sage`, normalizeWindowsHomeIdentity(`\\?\UNC\Server\Share\.sage\`))
}
