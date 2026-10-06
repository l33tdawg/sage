//go:build darwin && cgo

package shellcontrol

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/nativebootstrap"
	"github.com/stretchr/testify/require"
)

const nativePeerFixtureIdentifier = "com.sage.nativebootstrap.fixture"

// This helper is compiled and ad-hoc signed into a disposable directory. Its
// private key is synthetic test input; the production signing policy is untouched.
// No callback or mock replaces the socket peer verifier.
const nativePeerFixtureSource = `package main
import (
 "crypto/ed25519"
 "encoding/base64"
 "encoding/binary"
 "encoding/json"
 "fmt"
 "io"
 "net"
 "os"
 "strings"
 "syscall"
 "time"
)
type config struct { Endpoint, Generation, Origin, StartupProof, Mode, Seed, Replace, Replay string }
type result struct { Challenge, Response json.RawMessage; Error, Signature string }
func read(r io.Reader) ([]byte,error) { var h [4]byte; if _,e:=io.ReadFull(r,h[:]);e!=nil{return nil,e};n:=binary.BigEndian.Uint32(h[:]);if n==0||n>16384{return nil,fmt.Errorf("size")};p:=make([]byte,n);_,e:=io.ReadFull(r,p);return p,e }
func write(w io.Writer,p []byte) error { var h [4]byte;binary.BigEndian.PutUint32(h[:],uint32(len(p)));if _,e:=w.Write(h[:]);e!=nil{return e};_,e:=w.Write(p);return e }
func encode(v any) []byte { b,e:=json.Marshal(v);if e!=nil{panic(e)};return b }
func finish(r result) { if e:=json.NewEncoder(os.Stdout).Encode(r);e!=nil{panic(e)} }
func main() {
 if len(os.Args)>1&&os.Args[1]=="--hold" {
  f:=os.NewFile(3,"inherited");if f==nil{panic("missing inherited socket")};defer f.Close()
  fmt.Println("EXECUTED")
  first,e:=read(f);r:=result{Challenge:first};if e!=nil{r.Error=e.Error();finish(r);return}
  r.Response,e=read(f);if e!=nil{r.Error=e.Error()};finish(r);return
 }
 var c config;if e:=json.NewDecoder(os.Stdin).Decode(&c);e!=nil{panic(e)}
 seed,e:=base64.RawURLEncoding.DecodeString(c.Seed);if e!=nil{panic(e)};key:=ed25519.NewKeyFromSeed(seed)
 public:=base64.RawURLEncoding.EncodeToString(key.Public().(ed25519.PublicKey))
 conn,e:=net.Dial("unix",c.Endpoint);if e!=nil{panic(e)};defer conn.Close();conn.SetDeadline(time.Now().Add(5*time.Second))
 req:=map[string]any{"control_protocol":2,"shell_protocol":1,"operation":"native-session.issue","instance_generation":c.Generation,"ui_origin":c.Origin,"startup_proof":c.StartupProof,"public_key":public}
 if e=write(conn,encode(req));e!=nil{panic(e)}
 if c.Mode=="preexec" {
  f,e:=conn.(*net.UnixConn).File();if e!=nil{panic(e)}
  if e=syscall.Dup2(int(f.Fd()),3);e!=nil{panic(e)}
  if _,_,e:=syscall.Syscall(syscall.SYS_FCNTL,3,syscall.F_SETFD,0);e!=0{panic(e)}
  if e=syscall.SetNonblock(3,false);e!=nil{panic(e)}
  if e=syscall.Exec(c.Replace,[]string{c.Replace,"--hold"},os.Environ());e!=nil{panic(e)};return
 }
 if c.Mode=="prebuffered" { write(conn,encode(map[string]any{"control_protocol":2,"operation":"native-session.prove","signature":base64.RawURLEncoding.EncodeToString(make([]byte,64))})) }
 first,e:=read(conn);r:=result{Challenge:first};if e!=nil{r.Error=e.Error();finish(r);return}
 var challenge map[string]any;if e=json.Unmarshal(first,&challenge);e!=nil{panic(e)}
 if challenge["control_protocol"]!=float64(2)||challenge["operation"]!="native-session.prove"{panic("server issued before proof")}
 nonceText,ok:=challenge["challenge"].(string);if !ok{panic("missing nonce")}
 nonce,e:=base64.RawURLEncoding.DecodeString(nonceText);if e!=nil||len(nonce)!=32{panic("bad challenge")}
 if c.Mode!="no_proof"&&c.Mode!="prebuffered" {
  message:="SAGE-NATIVE-PEER/1\n"+nonceText+"\n"+c.Generation+"\n"+c.Origin+"\n"+c.StartupProof+"\n"+public
  if c.Mode=="wrong_nonce" {message=strings.Replace(message,nonceText,base64.RawURLEncoding.EncodeToString(make([]byte,32)),1)}
  if c.Mode=="wrong_binding" {message=strings.Replace(message,c.Origin,"http://localhost:1",1)}
  if c.Mode=="wrong_key" {key=ed25519.NewKeyFromSeed(make([]byte,32))}
  signature:=base64.RawURLEncoding.EncodeToString(ed25519.Sign(key,[]byte(message)))
  if c.Mode=="replay" {signature=c.Replay};if c.Mode=="padded"{signature+="="};r.Signature=signature
  proof:=map[string]any{"control_protocol":2,"operation":"native-session.prove","signature":signature}
  switch c.Mode {case "wrong_operation":proof["operation"]="native-session.issue";case "wrong_protocol":proof["control_protocol"]=1;case "unknown_field":proof["authority"]="admin";case "case_alias":proof["CONTROL_PROTOCOL"]=2;case "null_signature":proof["signature"]=nil}
  p:=encode(proof);if c.Mode=="duplicate_field"{p=[]byte(strings.Replace(string(p),"{","{\"control_protocol\":2,",1))}
  if e=write(conn,p);e!=nil{r.Error=e.Error();finish(r);return}
 }
 r.Response,e=read(conn);if e!=nil{r.Error=e.Error()};finish(r)
}
`

type nativeFixtureConfig struct {
	Endpoint, Generation, Origin, StartupProof, Mode, Seed, Replace, Replay string
}
type nativeFixtureResult struct {
	Challenge, Response json.RawMessage
	Error, Signature    string
}

func buildNativePeerFixture(t *testing.T) (binary, policy string) {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	binary = filepath.Join(dir, "peer")
	require.NoError(t, os.WriteFile(source, []byte(nativePeerFixtureSource), 0600))
	out, err := exec.Command("go", "build", "-o", binary, source).CombinedOutput()
	require.NoError(t, err, "%s", out)
	return binary, signNativePeerFixture(t, binary, nativePeerFixtureIdentifier)
}

func signNativePeerFixture(t *testing.T, binary, identifier string) string {
	t.Helper()
	out, err := exec.Command("/usr/bin/codesign", "--force", "--sign", "-", "--options", "runtime", "--identifier", identifier, binary).CombinedOutput()
	require.NoError(t, err, "%s", out)
	out, err = exec.Command("/usr/bin/codesign", "-d", "--verbose=4", binary).CombinedOutput()
	require.NoError(t, err, "%s", out)
	match := regexp.MustCompile("(?m)^CDHash=([a-f0-9]+)$").FindStringSubmatch(string(out))
	require.Len(t, match, 2)
	return fmt.Sprintf("identifier %q and cdhash H%q", identifier, match[1])
}

func nativeFixtureConfiguration(s *Server, mode string) nativeFixtureConfig {
	binding := s.NativeBinding()
	return nativeFixtureConfig{Endpoint: s.Endpoint(), Generation: binding.Generation, Origin: binding.Origin, StartupProof: binding.StartupProof, Mode: mode, Seed: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{42}, ed25519.SeedSize))}
}

func runNativePeerFixture(t *testing.T, binary string, config nativeFixtureConfig) nativeFixtureResult {
	t.Helper()
	input, err := json.Marshal(config)
	require.NoError(t, err)
	cmd := exec.Command(binary)
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	var result nativeFixtureResult
	require.NoError(t, json.Unmarshal(out, &result), "%s", out)
	normalizeNativeFixtureFrames(&result)
	return result
}

func normalizeNativeFixtureFrames(result *nativeFixtureResult) {
	if bytes.Equal(result.Challenge, []byte("null")) {
		result.Challenge = nil
	}
	if bytes.Equal(result.Response, []byte("null")) {
		result.Response = nil
	}
}

func startNativeFixtureServer(t *testing.T, policy string) *Server {
	t.Helper()
	s, err := Start(shortTempDir(t), "12.0.0-beta", "http://127.0.0.1:18080", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.EnableNativeBootstrap(policy))
	require.NoError(t, s.SetState(StateReady))
	return s
}

func TestNativeBootstrapRequiresFreshSameSocketProof(t *testing.T) {
	binary, policy := buildNativePeerFixture(t)
	s := startNativeFixtureServer(t, policy)
	valid := runNativePeerFixture(t, binary, nativeFixtureConfiguration(s, "valid"))
	require.Empty(t, valid.Error)
	var challenge nativePeerChallenge
	require.NoError(t, nativebootstrap.DecodeObject(valid.Challenge, &challenge))
	require.Equal(t, "native-session.prove", challenge.Operation)
	var issued nativeIssueResponse
	require.NoError(t, nativebootstrap.DecodeObject(valid.Response, &issued))
	require.Equal(t, "native-session.issue", issued.Operation)
	require.NotEmpty(t, issued.Ticket)
	redemption := nativebootstrap.Redemption{Ticket: issued.Ticket, Challenge: issued.Challenge, PublicKey: issued.PublicKey, Binding: s.NativeBinding()}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{42}, ed25519.SeedSize))
	redemption.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, nativebootstrap.SignatureMessage(redemption)))
	admission, err := s.NativeBootstrap().Redeem(redemption)
	require.NoError(t, err)
	require.NotEmpty(t, admission.Token)
	for _, mode := range []string{"wrong_nonce", "wrong_binding", "wrong_key", "wrong_operation", "wrong_protocol", "unknown_field", "case_alias", "duplicate_field", "null_signature", "padded", "prebuffered", "no_proof", "replay"} {
		t.Run(mode, func(t *testing.T) {
			cfg := nativeFixtureConfiguration(s, mode)
			cfg.Replay = valid.Signature
			started := time.Now()
			result := runNativePeerFixture(t, binary, cfg)
			require.NotEmpty(t, result.Challenge, "peer never reached the fresh challenge")
			require.Empty(t, result.Response, "server issued a ticket without valid fresh proof")
			require.NotEmpty(t, result.Error)
			if mode == "no_proof" {
				require.Less(t, time.Since(started), 4*time.Second, "server did not close the expired exchange")
			}
		})
	}
}

func TestNativeBootstrapRejectsBufferedRequestBeforePermittedExec(t *testing.T) {
	allowed, policy := buildNativePeerFixture(t)
	attacker := filepath.Join(t.TempDir(), "attacker")
	code, err := os.ReadFile(allowed)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(attacker, code, 0700))
	signNativePeerFixture(t, attacker, "com.sage.untrusted.fixture")
	// Gate handling until untrusted code has queued the request and exec'ed
	// permitted code, deterministically opening the original attack window.
	endpoint := filepath.Join(shortTempDir(t), "peer.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: endpoint, Net: "unix"})
	require.NoError(t, err)
	defer listener.Close()
	require.NoError(t, listener.SetDeadline(time.Now().Add(10*time.Second)))
	s := &Server{endpoint: endpoint, state: StateReady, origin: "http://127.0.0.1:18080", generation: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)), nativeChecks: make(chan struct{}, 4)}
	require.NoError(t, s.EnableNativeBootstrap(policy))
	cfg := nativeFixtureConfiguration(s, "preexec")
	cfg.Replace = allowed
	input, err := json.Marshal(cfg)
	require.NoError(t, err)
	cmd := exec.Command(attacker)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	conn, err := listener.AcceptUnix()
	require.NoError(t, err)
	defer conn.Close()
	scanner := bufio.NewScanner(out)
	require.True(t, scanner.Scan())
	require.Equal(t, "EXECUTED", scanner.Text())
	done := make(chan struct{})
	go func() { defer close(done); s.handle(conn) }()
	require.True(t, scanner.Scan())
	var result nativeFixtureResult
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &result))
	normalizeNativeFixtureFrames(&result)
	require.NoError(t, cmd.Wait(), "%s", stderr.String())
	<-done
	var challenge nativePeerChallenge
	require.NoError(t, nativebootstrap.DecodeObject(result.Challenge, &challenge))
	require.Equal(t, "native-session.prove", challenge.Operation)
	require.Empty(t, result.Response, "buffered attacker request received a ticket after permitted exec")
	require.NotEmpty(t, result.Error)
}

func TestNativeBootstrapRejectsNoncanonicalInitialBinding(t *testing.T) {
	binary, policy := buildNativePeerFixture(t)
	s := startNativeFixtureServer(t, policy)
	for _, mutate := range []func(*nativeFixtureConfig){
		func(c *nativeFixtureConfig) { c.Generation = strings.Repeat("A", 43) },
		func(c *nativeFixtureConfig) { c.Origin = "http://localhost:18080" },
		func(c *nativeFixtureConfig) { c.StartupProof = strings.Repeat("a", 64) },
	} {
		cfg := nativeFixtureConfiguration(s, "valid")
		mutate(&cfg)
		result := runNativePeerFixture(t, binary, cfg)
		require.Empty(t, result.Challenge)
		require.Empty(t, result.Response)
		require.NotEmpty(t, result.Error)
	}
}
