// Package handoff implements the Rust original's zero-downtime reload:
// LISTEN_FDS socket inheritance, a Unix control socket used by the successor,
// sd_notify handshaking, and a 15-minute drain of in-flight connections.
//
// There are two halves. A running server calls Accept with its api/metrics
// listeners and a drain channel; `handoff` (invoked by systemd ExecReload)
// calls Request with the argv of the replacement process. The old process
// spawns the successor with its sockets on fd 3, 4, ..., and only replies
// "ok" once the drain begins (i.e. once systemd/SIGTERM has confirmed the
// successor took over).
package handoff

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// ListenFDsStart is the first descriptor of the LISTEN_FDS protocol.
	ListenFDsStart = 3
	// PredecessorEnv names the pid a successor should relieve with SIGTERM.
	PredecessorEnv = "SLOP_PROXY_PREDECESSOR"
	// ReadyLimit bounds how long the old process waits for its successor.
	ReadyLimit = 2 * time.Minute
	// DrainLimit bounds how long a draining process keeps serving open streams.
	DrainLimit = 15 * time.Minute
	// ControlName is the socket file inside $RUNTIME_DIRECTORY.
	ControlName = "control"
)

// Socket identifies a listener passed across the handoff boundary.
type Socket int

const (
	SocketAPI Socket = iota
	SocketMetrics
)

func (s Socket) String() string {
	switch s {
	case SocketAPI:
		return "api"
	case SocketMetrics:
		return "metrics"
	}
	return ""
}

func parseSocket(name string) (Socket, bool) {
	switch name {
	case "api":
		return SocketAPI, true
	case "metrics":
		return SocketMetrics, true
	}
	return 0, false
}

// NamedListener is a socket a running server can hand to its successor.
type NamedListener struct {
	Socket   Socket
	Listener net.Listener
}

// ControlPath returns $RUNTIME_DIRECTORY/control, or "" when unset. Without a
// runtime directory there is nothing to hand off from and Accept is a no-op.
func ControlPath() string {
	if runtime.GOOS == "windows" {
		return ""
	}
	dir := os.Getenv("RUNTIME_DIRECTORY")
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, ControlName)
}

// Inherited holds listeners passed in via the systemd LISTEN_FDS protocol.
type Inherited struct {
	Listeners map[Socket]net.Listener
}

// Take claims inherited fds. LISTEN_PID, when present, must match this process
// (systemd sets it on a first start). A successor spawned by a predecessor has
// LISTEN_PID removed, so its inherited fds are ours unconditionally.
func Take() *Inherited {
	out := &Inherited{Listeners: make(map[Socket]net.Listener)}
	if runtime.GOOS == "windows" {
		return out
	}
	if pid := os.Getenv("LISTEN_PID"); pid != "" && pid != strconv.Itoa(os.Getpid()) {
		return out
	}
	count, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || count < 1 {
		return out
	}
	names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")
	for i := 0; i < count; i++ {
		fd := uintptr(ListenFDsStart + i)
		name := ""
		if i < len(names) {
			name = names[i]
		}
		// Never wrap (and later close) an fd that is not a socket: a stale
		// LISTEN_FDS in the environment could otherwise close an unrelated
		// descriptor that happens to be number 3.
		if !isSocket(fd) {
			slog.Warn("ignoring inherited non-socket fd", "fd", fd)
			continue
		}
		file := os.NewFile(fd, "inherited-"+strconv.Itoa(i))
		if file == nil {
			continue
		}
		sock, ok := parseSocket(name)
		if !ok {
			slog.Warn("ignoring inherited socket", "name", name, "fd", fd)
			_ = file.Close()
			continue
		}
		ln, err := net.FileListener(file)
		_ = file.Close()
		if err != nil {
			slog.Warn("cannot adopt inherited socket", "name", name, "error", err)
			continue
		}
		out.Listeners[sock] = ln
	}
	return out
}

// Listen adopts an inherited listener for socket, or binds addr when absent.
func (i *Inherited) Listen(socket Socket, addr string) (net.Listener, error) {
	if ln, ok := i.Listeners[socket]; ok {
		delete(i.Listeners, socket)
		slog.Info("serving on inherited socket", "socket", socket.String())
		return ln, nil
	}
	return net.Listen("tcp", addr)
}

// CloseRemaining releases inherited sockets that were not consumed (for
// example a metrics socket when metrics_bind is unset).
func (i *Inherited) CloseRemaining() {
	for _, ln := range i.Listeners {
		_ = ln.Close()
	}
	i.Listeners = make(map[Socket]net.Listener)
}

// NotifyReady tells systemd we are the new main pid and are ready. The two
// lines let systemd retarget MAINPID, which is what makes it SIGTERM the
// predecessor. Failure is only a warning, matching the Rust original.
func NotifyReady() {
	if runtime.GOOS == "windows" {
		return
	}
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return
	}
	// A leading @ denotes a Linux abstract-namespace socket; Go's unixgram
	// dialer understands that form on Linux. On other platforms the path is
	// used verbatim and the send simply fails with a warning.
	conn, err := net.Dial("unixgram", sock)
	if err != nil {
		slog.Warn("notifying systemd", "error", err)
		return
	}
	defer conn.Close()
	state := "MAINPID=" + strconv.Itoa(os.Getpid()) + "\nREADY=1"
	if _, err := conn.Write([]byte(state)); err != nil {
		slog.Warn("notifying systemd", "error", err)
	}
}

// RelievePredecessor sends SIGTERM to the pid recorded by our predecessor.
// This is the belt-and-braces companion to systemd's own SIGTERM.
func RelievePredecessor() {
	if runtime.GOOS == "windows" {
		return
	}
	pid, err := strconv.Atoi(os.Getenv(PredecessorEnv))
	if err != nil || pid <= 0 {
		return
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		slog.Warn("signalling predecessor", "pid", pid, "error", err)
	}
}

// Request asks the running server to start argv as its successor on the
// inherited sockets. The request is NUL-joined argv; the reply is exactly
// "ok" on success or a human-readable error string.
func Request(argv []string) error {
	if runtime.GOOS == "windows" {
		return errors.New("socket handoff is not supported on Windows")
	}
	path := ControlPath()
	if path == "" {
		return errors.New("RUNTIME_DIRECTORY is unset, nothing to hand off from")
	}
	if len(argv) == 0 || argv[0] == "" {
		return errors.New("empty handoff request")
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", path, err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, strings.Join(argv, "\x00")); err != nil {
		return err
	}
	// Shut down the write half: the server uses EOF as the frame delimiter.
	if unix, ok := conn.(*net.UnixConn); ok {
		_ = unix.CloseWrite()
	}
	reply, err := io.ReadAll(conn)
	if err != nil {
		return err
	}
	if string(reply) != "ok" {
		return errors.New(string(reply))
	}
	return nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// Accept starts a Unix control socket that hands the supplied listeners to a
// successor. drain is closed when this process begins draining; that is what
// tells a pending handoff that the successor has taken over. The returned
// closer shuts the control socket down.
func Accept(sockets []NamedListener, drain <-chan struct{}) (io.Closer, error) {
	path := ControlPath()
	if path == "" {
		return nopCloser{}, nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("removing stale control socket: %w", err)
	}
	control, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("binding handoff socket %s: %w", path, err)
	}
	// The successor rebinds this very path while we are still draining. Go's
	// UnixListener otherwise unlinks the path on Close and would delete the
	// successor's control socket. Rust's listener does not unlink, so keep
	// the file and let the next process remove its own stale socket.
	if unix, ok := control.(*net.UnixListener); ok {
		unix.SetUnlinkOnClose(false)
	}
	// Take a dup of each listener up front. TCPListener.File() leaves the
	// running server's own listener intact while giving the successor a copy.
	files := make([]*os.File, 0, len(sockets))
	names := make([]string, 0, len(sockets))
	for _, s := range sockets {
		f, err := fileFromListener(s.Listener)
		if err != nil {
			for _, opened := range files {
				_ = opened.Close()
			}
			_ = control.Close()
			return nil, fmt.Errorf("duplicating %s socket: %w", s.Socket.String(), err)
		}
		files = append(files, f)
		names = append(names, s.Socket.String())
	}
	go acceptLoop(control, files, names, drain)
	return control, nil
}

func fileFromListener(ln net.Listener) (*os.File, error) {
	type fileer interface {
		File() (*os.File, error)
	}
	if f, ok := ln.(fileer); ok {
		return f.File()
	}
	return nil, fmt.Errorf("listener %T cannot be inherited", ln)
}

func acceptLoop(control net.Listener, files []*os.File, names []string, drain <-chan struct{}) {
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	for {
		conn, err := control.Accept()
		if err != nil {
			return
		}
		if err := handOff(conn, files, names, drain); err != nil {
			slog.Error("handoff failed", "error", err)
			_, _ = io.WriteString(conn, err.Error())
			_ = conn.Close()
			continue
		}
		_, _ = io.WriteString(conn, "ok")
		_ = conn.Close()
		_ = control.Close()
		return // one successful handoff is enough
	}
}

func handOff(conn net.Conn, files []*os.File, names []string, drain <-chan struct{}) error {
	request, err := io.ReadAll(conn)
	if err != nil {
		return err
	}
	argv := strings.Split(string(request), "\x00")
	if len(argv) == 0 || argv[0] == "" {
		return errors.New("empty handoff request")
	}
	slog.Info("handing off", "program", argv[0])
	child, err := spawnSuccessor(argv, files, names)
	if err != nil {
		return err
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()
	timer := time.NewTimer(ReadyLimit)
	defer timer.Stop()
	select {
	case err := <-waited:
		if err != nil {
			return fmt.Errorf("successor exited before it was ready: %w", err)
		}
		return errors.New("successor exited before it was ready")
	case <-drain:
		return nil
	case <-timer.C:
		_ = child.Process.Kill()
		return fmt.Errorf("successor was not ready within %ds", int(ReadyLimit.Seconds()))
	}
}

func spawnSuccessor(argv []string, files []*os.File, names []string) (*exec.Cmd, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	// os/exec places ExtraFiles on fd 3, 4, ... and clears FD_CLOEXEC for us,
	// which is exactly the LISTEN_FDS protocol (fd 3 is LISTEN_FDS_START).
	cmd.ExtraFiles = files
	cmd.Env = successorEnv(len(files), strings.Join(names, ":"))
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting successor: %w", err)
	}
	return cmd, nil
}

// successorEnv carries the LISTEN_FDS contract to the child. LISTEN_PID is
// removed: it belongs to the process that spawned us, not to the successor.
func successorEnv(count int, names string) []string {
	env := make([]string, 0, len(os.Environ())+3)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "LISTEN_PID=") ||
			strings.HasPrefix(kv, "LISTEN_FDS=") ||
			strings.HasPrefix(kv, "LISTEN_FDNAMES=") ||
			strings.HasPrefix(kv, PredecessorEnv+"=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"LISTEN_FDS="+strconv.Itoa(count),
		"LISTEN_FDNAMES="+names,
		PredecessorEnv+"="+strconv.Itoa(os.Getpid()),
	)
}
