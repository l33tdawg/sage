package shellcontrol

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	ControlProtocol = 1
	ShellProtocol   = 1
	APISchema       = 1
	maxFrameBytes   = 16 * 1024
)

type State string

const (
	StateStarting State = "starting"
	StateLocked   State = "locked"
	StateReady    State = "ready"
	StateDegraded State = "degraded"
	StateDraining State = "draining"
	StateFailed   State = "failed"
)

type Request struct {
	ControlProtocol int    `json:"control_protocol"`
	ShellProtocol   int    `json:"shell_protocol"`
	Operation       string `json:"operation"`
}

type Response struct {
	ControlProtocol    int    `json:"control_protocol"`
	DaemonVersion      string `json:"daemon_version"`
	APISchema          int    `json:"api_schema"`
	MinShellProtocol   int    `json:"min_shell_protocol"`
	MaxShellProtocol   int    `json:"max_shell_protocol"`
	InstanceGeneration string `json:"instance_generation"`
	State              State  `json:"state"`
	UIOrigin           string `json:"ui_origin,omitempty"`
	StartupProof       string `json:"startup_proof,omitempty"`
}

type Server struct {
	listener     net.Listener
	endpoint     string
	cleanup      func() error
	version      string
	origin       string
	generation   string
	startupProof string

	mu    sync.RWMutex
	state State
	done  chan struct{}
	once  sync.Once
}

func Start(sageHome, daemonVersion, uiOrigin, startupProof string) (*Server, error) {
	origin, err := canonicalLoopbackOrigin(uiOrigin)
	if err != nil {
		return nil, err
	}
	if startupProof != "" && !validStartupProof(startupProof) {
		return nil, fmt.Errorf("native-shell startup proof must be a lowercase SHA-256 hex digest")
	}
	generationRaw := make([]byte, 32)
	if _, randErr := rand.Read(generationRaw); randErr != nil {
		return nil, fmt.Errorf("generate native-shell instance generation: %w", randErr)
	}
	listener, endpoint, cleanup, err := listenEndpoint(sageHome)
	if err != nil {
		return nil, err
	}
	s := &Server{
		listener: listener, endpoint: endpoint, cleanup: cleanup, version: daemonVersion,
		origin: origin, generation: base64.RawURLEncoding.EncodeToString(generationRaw), startupProof: startupProof,
		state: StateStarting, done: make(chan struct{}),
	}
	go s.serve()
	return s, nil
}

func (s *Server) Endpoint() string { return s.endpoint }

func (s *Server) SetState(state State) error {
	switch state {
	case StateStarting, StateLocked, StateReady, StateDegraded, StateDraining, StateFailed:
	default:
		return fmt.Errorf("invalid native-shell state %q", state)
	}
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
	return nil
}

func (s *Server) Close() error {
	var err error
	s.once.Do(func() {
		// Unlink the endpoint while our listener is still open, so cleanup can
		// verify the path still names the socket we created. Unix listeners have
		// automatic unlink disabled; otherwise Close could delete a replacement
		// socket created by another process.
		cleanupErr := s.cleanup()
		err = s.listener.Close()
		<-s.done
		if err == nil {
			err = cleanupErr
		}
	})
	return err
}

func (s *Server) serve() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := verifyPeer(conn); err != nil {
		return
	}
	payload, readErr := readFrame(conn)
	if readErr != nil {
		return
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	var req Request
	if decodeErr := decoder.Decode(&req); decodeErr != nil || decoder.More() {
		return
	}
	var trailing any
	if trailingErr := decoder.Decode(&trailing); !errors.Is(trailingErr, io.EOF) {
		return
	}
	if req.ControlProtocol != ControlProtocol || req.ShellProtocol != ShellProtocol || req.Operation != "status" {
		return
	}

	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	resp := Response{
		ControlProtocol: ControlProtocol, DaemonVersion: s.version, APISchema: APISchema,
		MinShellProtocol: ShellProtocol, MaxShellProtocol: ShellProtocol,
		InstanceGeneration: s.generation, State: state,
		StartupProof: s.startupProof,
	}
	if state == StateReady || state == StateDegraded {
		resp.UIOrigin = s.origin
	}
	encoded, err := json.Marshal(resp)
	if err == nil {
		_ = writeFrame(conn, encoded)
	}
}

func validStartupProof(proof string) bool {
	if len(proof) != 64 {
		return false
	}
	for _, char := range proof {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxFrameBytes {
		return nil, fmt.Errorf("native-shell control frame size %d is invalid", size)
	}
	payload := make([]byte, int(size))
	_, err := io.ReadFull(r, payload)
	return payload, err
}

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) == 0 || len(payload) > maxFrameBytes {
		return fmt.Errorf("native-shell control response size %d is invalid", len(payload))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func canonicalLoopbackOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return "", fmt.Errorf("native-shell UI origin must be a plain loopback HTTP origin")
	}
	host := strings.ToLower(u.Hostname())
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return "", fmt.Errorf("native-shell UI origin host %q is not loopback", host)
	}
	if u.Port() == "" {
		return "", fmt.Errorf("native-shell UI origin requires an explicit port")
	}
	return u.Scheme + "://" + u.Host, nil
}
