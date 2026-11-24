package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
)

func TestExitRotationRefreshesSharedHosts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("rotation hooks use /bin/sh")
	}
	var calls atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
	}))
	defer proxy.Close()
	cfg := config.EgressConfig{Exits: []config.ExitConfig{{ProxyURL: proxy.URL, RotateCommand: "exit 0"}}}
	exits := NewExits(cfg)
	primary, err := NewSharedEgressPool(cfg, "primary", "", exits)
	if err != nil {
		t.Fatal(err)
	}
	secondary, err := NewSharedEgressPool(cfg, "secondary", "", exits)
	if err != nil {
		t.Fatal(err)
	}
	build := func(_ *http.Client) (*http.Request, error) {
		return http.NewRequest("GET", "http://loopback-test.invalid", nil)
	}
	if _, failure := primary.SendAnonymous(context.Background(), build); failure == nil || failure.Kind != ErrRateLimited {
		t.Fatalf("rate-limited exit = %v", failure)
	}
	deadline := time.Now().Add(3 * time.Second)
	for exits[0].generation.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("rotation hook did not complete")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, host := range []*EgressPool{primary, secondary} {
		resp, failure := host.SendAnonymous(context.Background(), build)
		if failure != nil {
			t.Fatal(failure)
		}
		_ = resp.Body.Close()
		if host.exits[0].generation != 1 {
			t.Fatal("host retained the previous exit client")
		}
	}
}

func TestProxyFileReloadKeepsExplicitRouting(t *testing.T) {
	var direct, proxied atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		direct.Add(1)
		w.WriteHeader(200)
	}))
	defer origin.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxied.Add(1)
		w.WriteHeader(200)
	}))
	defer proxy.Close()
	path := filepath.Join(t.TempDir(), "proxies.txt")
	write := func(value string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(fmt.Sprintf("# configured route\n\n%s\n", proxy.URL))
	pool, err := NewEgressPool(config.EgressConfig{ProxyURLsFile: path}, "test", "")
	if err != nil {
		t.Fatal(err)
	}
	send := func() *SendError {
		resp, failure := pool.Send(context.Background(), func(_ *http.Client) (*http.Request, error) {
			return http.NewRequest("GET", origin.URL, nil)
		})
		if resp != nil {
			_ = resp.Body.Close()
		}
		return failure
	}
	if failure := send(); failure != nil {
		t.Fatal(failure)
	}
	write("invalid-url")
	if err := pool.Reload(); err == nil {
		t.Fatal("invalid list accepted")
	}
	if failure := send(); failure != nil {
		t.Fatal(failure)
	}
	write("")
	if err := pool.Reload(); err != nil {
		t.Fatal(err)
	}
	if failure := send(); failure == nil {
		t.Fatal("empty configured list still sent a request")
	}
	if direct.Load() != 0 || proxied.Load() != 2 {
		t.Fatalf("requests direct=%d proxy=%d", direct.Load(), proxied.Load())
	}
}
