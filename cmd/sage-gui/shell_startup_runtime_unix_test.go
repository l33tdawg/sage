//go:build !windows

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is deliberately a short-lived daemon smoke, not an in-process mock: it
// proves that serve consumes the inherited challenge, starts the real SSCP
// listener, and publishes only the expected hash before node startup proceeds.
func TestShellStartupProofRuntimePublishesSSCPProof(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "sage-gui")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = os.Environ()
	output, err := build.CombinedOutput()
	require.NoErrorf(t, err, "build runtime smoke binary: %s", output)

	home := shortShellStartupHome(t)
	challenge := bytes.Repeat([]byte{0x5A}, shellStartupChallengeBytes)
	expectedProof, err := shellStartupProof(bytes.NewReader(challenge), "1")
	require.NoError(t, err)

	command := exec.Command(binary, "serve")
	command.Env = append(
		os.Environ(),
		"SAGE_HOME="+home,
		shellStartupChallengeEnv+"=1",
		"SAGE_NO_BROWSER=1",
		"REST_ADDR="+shellStartupRESTAddr(t),
	)
	stdin, err := command.StdinPipe()
	require.NoError(t, err)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	require.NoError(t, command.Start())
	_, err = stdin.Write(challenge)
	require.NoError(t, err)
	require.NoError(t, stdin.Close())

	finished := false
	t.Cleanup(func() {
		if !finished && command.Process != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})

	status, err := waitForShellStatus(filepath.Join(home, "run", "shell-control.sock"), 8*time.Second)
	if err != nil {
		_ = command.Process.Kill()
		waitErr := command.Wait()
		finished = true
		t.Fatalf("wait for shell status: %v; reap daemon: %v; daemon stderr: %s", err, waitErr, stderr.String())
	}
	assert.Equal(t, "starting", status.State)
	assert.Equal(t, expectedProof, status.StartupProof)

	require.NoError(t, command.Process.Kill())
	err = command.Wait()
	finished = true
	require.Error(t, err)
}

type shellStartupStatus struct {
	State        string `json:"state"`
	StartupProof string `json:"startup_proof"`
}

func waitForShellStatus(endpoint string, timeout time.Duration) (shellStartupStatus, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("unix", endpoint, 100*time.Millisecond)
		if err == nil {
			status, requestErr := requestShellStartupStatus(connection)
			_ = connection.Close()
			if requestErr == nil {
				return status, nil
			}
			lastErr = requestErr
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	return shellStartupStatus{}, lastErr
}

func requestShellStartupStatus(connection net.Conn) (shellStartupStatus, error) {
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return shellStartupStatus{}, err
	}
	request, err := json.Marshal(map[string]any{
		"control_protocol": 1,
		"shell_protocol":   1,
		"operation":        "status",
	})
	if err != nil {
		return shellStartupStatus{}, err
	}
	if writeErr := writeShellStartupFrame(connection, request); writeErr != nil {
		return shellStartupStatus{}, writeErr
	}
	response, err := readShellStartupFrame(connection)
	if err != nil {
		return shellStartupStatus{}, err
	}
	var status shellStartupStatus
	if unmarshalErr := json.Unmarshal(response, &status); unmarshalErr != nil {
		return shellStartupStatus{}, unmarshalErr
	}
	return status, nil
}

func writeShellStartupFrame(writer io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	_, err := writer.Write(payload)
	return err
}

func readShellStartupFrame(reader io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header[:]))
	_, err := io.ReadFull(reader, payload)
	return payload, err
}

func shortShellStartupHome(t *testing.T) string {
	t.Helper()
	base := "/tmp"
	if info, err := os.Stat("/private/tmp"); err == nil && info.IsDir() {
		base = "/private/tmp"
	}
	home, err := os.MkdirTemp(base, "sage-sscp-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(home)) })
	return home
}

func shellStartupRESTAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
