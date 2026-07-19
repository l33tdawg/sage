//go:build !windows

package shellcontrol

import (
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusProtocolAndEndpointPermissions(t *testing.T) {
	home := shortTempDir(t)
	server, err := Start(home, "11.11.0-test", "http://127.0.0.1:8080")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	runInfo, err := os.Stat(filepath.Join(home, "run"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0700), runInfo.Mode().Perm())
	socketInfo, err := os.Stat(server.Endpoint())
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), socketInfo.Mode().Perm())

	starting := requestStatus(t, server.Endpoint())
	assert.Equal(t, StateStarting, starting.State)
	assert.Empty(t, starting.UIOrigin)
	require.NoError(t, server.SetState(StateReady))
	ready := requestStatus(t, server.Endpoint())
	assert.Equal(t, StateReady, ready.State)
	assert.Equal(t, "http://127.0.0.1:8080", ready.UIOrigin)
	assert.Len(t, ready.InstanceGeneration, 43)
}

func TestRejectsUnknownAndOversizedFrames(t *testing.T) {
	server, err := Start(shortTempDir(t), "dev", "http://localhost:8080")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	for _, payload := range [][]byte{
		[]byte(`{"control_protocol":1,"shell_protocol":1,"operation":"status","authority":"admin"}`),
	} {
		conn, err := net.Dial("unix", server.Endpoint())
		require.NoError(t, err)
		require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
		assert.NoError(t, writeFrame(conn, payload))
		_, err = readFrame(conn)
		assert.Error(t, err)
		require.NoError(t, conn.Close())
	}
	conn, err := net.Dial("unix", server.Endpoint())
	require.NoError(t, err)
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], maxFrameBytes+1)
	_, err = conn.Write(header[:])
	require.NoError(t, err)
	_, err = readFrame(conn)
	assert.Error(t, err)
	require.NoError(t, conn.Close())
}

func TestRejectsNonLoopbackOrDecoratedOrigin(t *testing.T) {
	for _, origin := range []string{
		"https://127.0.0.1:8080", "http://192.168.1.2:8080", "http://127.0.0.1:8080/ui", "http://user@localhost:8080",
	} {
		_, err := Start(shortTempDir(t), "dev", origin)
		assert.Error(t, err, origin)
	}
}

func TestCleanupDoesNotRemoveReplacementSocket(t *testing.T) {
	listener, endpoint, cleanup, err := listenEndpoint(shortTempDir(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	require.NoError(t, os.Remove(endpoint))
	replacement, err := net.Listen("unix", endpoint)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, replacement.Close())
	})
	assert.ErrorContains(t, cleanup(), "replaced")
	_, err = os.Lstat(endpoint)
	assert.NoError(t, err)
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	base := "/tmp"
	if info, err := os.Stat("/private/tmp"); err == nil && info.IsDir() {
		base = "/private/tmp"
	}
	dir, err := os.MkdirTemp(base, "sage-sc-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	return dir
}

func requestStatus(t *testing.T, endpoint string) Response {
	t.Helper()
	conn, err := net.Dial("unix", endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	payload, err := json.Marshal(Request{ControlProtocol: 1, ShellProtocol: 1, Operation: "status"})
	require.NoError(t, err)
	require.NoError(t, writeFrame(conn, payload))
	responsePayload, err := readFrame(conn)
	require.NoError(t, err)
	var response Response
	require.NoError(t, json.Unmarshal(responsePayload, &response))
	return response
}
