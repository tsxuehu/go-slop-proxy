//go:build !windows

package handoff

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestMain lets the handoff test binary double as the successor process. The
// parent sets GO_WANT_HANDOFF_HELPER=1 and spawns os.Executable() with the
// LISTEN_FDS environment; the helper verifies it inherited the sockets.
func TestMain(m *testing.M) {
	if os.Getenv("GO_WANT_HANDOFF_HELPER") == "1" {
		runHandoffHelper()
		return
	}
	os.Exit(m.Run())
}

type helperInfo struct {
	ListenFDs     string `json:"listen_fds"`
	ListenFDNames string `json:"listen_fdnames"`
	ListenPID     string `json:"listen_pid"`
	Predecessor   string `json:"predecessor"`
	PPID          int    `json:"ppid"`
	API           bool   `json:"api"`
	Metrics       bool   `json:"metrics"`
	APIAddr       string `json:"api_addr"`
	MetricsAddr   string `json:"metrics_addr"`
}

func runHandoffHelper() {
	taken := Take()
	info := helperInfo{
		ListenFDs:     os.Getenv("LISTEN_FDS"),
		ListenFDNames: os.Getenv("LISTEN_FDNAMES"),
		ListenPID:     os.Getenv("LISTEN_PID"),
		Predecessor:   os.Getenv(PredecessorEnv),
		PPID:          os.Getppid(),
	}
	if ln := taken.Listeners[SocketAPI]; ln != nil {
		info.API, info.APIAddr = true, ln.Addr().String()
		_ = ln.Close()
	}
	if ln := taken.Listeners[SocketMetrics]; ln != nil {
		info.Metrics, info.MetricsAddr = true, ln.Addr().String()
		_ = ln.Close()
	}
	b, _ := json.Marshal(info)
	_ = os.WriteFile(os.Getenv("HELPER_OUT"), b, 0o600)
	_ = os.WriteFile(os.Getenv("HELPER_READY"), []byte("ok"), 0o600)
	// Stay alive until the parent has collected the "ok" reply, so the
	// successor-exited-before-ready branch cannot win the race.
	quit := os.Getenv("HELPER_QUIT")
	if quit == "" {
		quit = filepath.Join(filepath.Dir(os.Getenv("HELPER_OUT")), "helper.quit")
	}
	done := filepath.Join(filepath.Dir(os.Getenv("HELPER_OUT")), "helper.done")
	for i := 0; i < 150; i++ {
		if _, err := os.Stat(quit); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Signal before exiting so the parent cannot tear down TempDir (and thus
	// the quit file) while we are still checking for it.
	_ = os.WriteFile(done, []byte("ok"), 0o600)
	os.Exit(0)
}

func TestSocketNames(t *testing.T) {
	if SocketAPI.String() != "api" || SocketMetrics.String() != "metrics" {
		t.Fatal("unexpected socket names")
	}
	for name, want := range map[string]Socket{"api": SocketAPI, "metrics": SocketMetrics} {
		got, ok := parseSocket(name)
		if !ok || got != want {
			t.Errorf("parseSocket(%q) = %v,%v", name, got, ok)
		}
	}
	if _, ok := parseSocket("unknown"); ok {
		t.Error("unknown socket name was accepted")
	}
}

func TestControlPath(t *testing.T) {
	t.Setenv("RUNTIME_DIRECTORY", "")
	if got := ControlPath(); got != "" {
		t.Errorf("empty runtime directory produced %q", got)
	}
	t.Setenv("RUNTIME_DIRECTORY", "/run/slop-proxy")
	if got, want := ControlPath(), filepath.Join("/run/slop-proxy", "control"); got != want {
		t.Errorf("ControlPath() = %q, want %q", got, want)
	}
}

func TestRequestWithoutRuntimeDirectory(t *testing.T) {
	t.Setenv("RUNTIME_DIRECTORY", "")
	err := Request([]string{"/bin/true"})
	if err == nil || !contains(err.Error(), "RUNTIME_DIRECTORY is unset") {
		t.Fatalf("Request error = %v", err)
	}
}

func TestTakeIgnoresForeignListenPID(t *testing.T) {
	t.Setenv("LISTEN_PID", "1")
	t.Setenv("LISTEN_FDS", "2")
	t.Setenv("LISTEN_FDNAMES", "api:metrics")
	if got := Take(); len(got.Listeners) != 0 {
		t.Errorf("adopted %d sockets from a foreign LISTEN_PID", len(got.Listeners))
	}
}

func TestInheritedListenBindsWhenAbsent(t *testing.T) {
	i := &Inherited{Listeners: map[Socket]net.Listener{}}
	ln, err := i.Listen(SocketAPI, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if ln.Addr().String() == "" {
		t.Error("bound listener has no address")
	}
}

func TestSuccessorEnv(t *testing.T) {
	t.Setenv("LISTEN_PID", "999")
	t.Setenv("LISTEN_FDS", "9")
	t.Setenv("LISTEN_FDNAMES", "old")
	t.Setenv(PredecessorEnv, "888")
	env := successorEnv(2, "api:metrics")
	got := map[string]string{}
	for _, kv := range env {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				got[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	if got["LISTEN_PID"] != "" {
		t.Errorf("LISTEN_PID was not removed: %q", got["LISTEN_PID"])
	}
	if got["LISTEN_FDS"] != "2" || got["LISTEN_FDNAMES"] != "api:metrics" {
		t.Errorf("descriptor env: %q / %q", got["LISTEN_FDS"], got["LISTEN_FDNAMES"])
	}
	if got[PredecessorEnv] != strconv.Itoa(os.Getpid()) {
		t.Errorf("predecessor = %q, want our pid", got[PredecessorEnv])
	}
}

func TestNotifyReady(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "notify.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	t.Setenv("NOTIFY_SOCKET", sock)
	NotifyReady()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	want := "MAINPID=" + strconv.Itoa(os.Getpid()) + "\nREADY=1"
	if string(buf[:n]) != want {
		t.Errorf("sd_notify payload = %q, want %q", string(buf[:n]), want)
	}
}

// TestAcceptHandsOffSockets drives the whole control-socket handshake with a
// real successor process: NUL argv -> spawn with fds -> drain -> "ok" reply,
// and verifies the successor adopted both listeners at their original addrs.
func TestAcceptHandsOffSockets(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RUNTIME_DIRECTORY", dir)

	apiLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer apiLn.Close()
	metricsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsLn.Close()
	apiAddr, metricsAddr := apiLn.Addr().String(), metricsLn.Addr().String()

	drain := make(chan struct{})
	closer, err := Accept([]NamedListener{
		{Socket: SocketAPI, Listener: apiLn},
		{Socket: SocketMetrics, Listener: metricsLn},
	}, drain)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()

	out := filepath.Join(dir, "helper.json")
	ready := filepath.Join(dir, "helper.ready")
	quit := filepath.Join(dir, "helper.quit")
	t.Setenv("HELPER_OUT", out)
	t.Setenv("HELPER_READY", ready)
	t.Setenv("HELPER_QUIT", quit)
	t.Setenv("GO_WANT_HANDOFF_HELPER", "1")

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- Request([]string{exe}) }()

	waitForFile(t, ready, 10*time.Second)
	close(drain) // systemd/SIGTERM confirming the successor took over
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("handoff request: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handoff request timed out")
	}
	_ = os.WriteFile(quit, []byte("ok"), 0o600)
	waitForFile(t, filepath.Join(dir, "helper.done"), 5*time.Second)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var info helperInfo
	if err := json.Unmarshal(b, &info); err != nil {
		t.Fatal(err)
	}
	if info.ListenFDs != "2" || info.ListenFDNames != "api:metrics" {
		t.Errorf("successor env: LISTEN_FDS=%q LISTEN_FDNAMES=%q", info.ListenFDs, info.ListenFDNames)
	}
	if info.ListenPID != "" {
		t.Errorf("successor still has LISTEN_PID=%q", info.ListenPID)
	}
	if info.Predecessor != strconv.Itoa(os.Getpid()) {
		t.Errorf("predecessor = %q, want %d", info.Predecessor, os.Getpid())
	}
	if info.PPID != os.Getpid() {
		t.Errorf("successor ppid = %d, want %d", info.PPID, os.Getpid())
	}
	if !info.API || !info.Metrics {
		t.Fatalf("successor adopted api=%v metrics=%v", info.API, info.Metrics)
	}
	if info.APIAddr != apiAddr || info.MetricsAddr != metricsAddr {
		t.Errorf("successor addrs = %q / %q, want %q / %q",
			info.APIAddr, info.MetricsAddr, apiAddr, metricsAddr)
	}
}

func waitForFile(t *testing.T, path string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
