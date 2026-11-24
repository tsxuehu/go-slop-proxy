package server

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func zstdCompress(t *testing.T, plain []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestZstdBodyReachesHandlerAsJSON(t *testing.T) {
	plain := []byte(`{"model":"m"}`)
	var seenLen int64
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenLen = r.ContentLength
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	})
	h := decompressMiddleware(inner)
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/responses", bytes.NewReader(zstdCompress(t, plain)))
	req.Header.Set("Content-Encoding", "zstd")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != string(plain) {
		t.Fatalf("got %d %q, want plain JSON", rec.Code, rec.Body.String())
	}
	if seenLen != int64(len(plain)) {
		t.Errorf("ContentLength = %d, want %d", seenLen, len(plain))
	}
}

func TestNonZstdBodyPassesThrough(t *testing.T) {
	plain := []byte(`{"model":"m"}`)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	})
	rec := httptest.NewRecorder()
	decompressMiddleware(inner).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/responses", bytes.NewReader(plain)))
	if rec.Body.String() != string(plain) {
		t.Fatalf("got %q", rec.Body.String())
	}
}

func TestMalformedZstdReturns400(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/responses", bytes.NewReader([]byte("not a zstd frame")))
	req.Header.Set("Content-Encoding", "zstd")
	rec := httptest.NewRecorder()
	decompressMiddleware(inner).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 (%s)", rec.Code, rec.Body.String())
	}
}

func TestDecodeZstdOversizeIsRefused(t *testing.T) {
	frame := zstdCompress(t, bytes.Repeat([]byte("a"), 65))
	if _, err := decodeZstd(frame, 64); !errors.Is(err, errZstdTooLarge) {
		t.Fatalf("oversize decode err = %v, want errZstdTooLarge", err)
	}
	fitting := zstdCompress(t, bytes.Repeat([]byte("a"), 64))
	out, err := decodeZstd(fitting, 64)
	if err != nil || len(out) != 64 {
		t.Fatalf("fitting decode = %d bytes, err %v", len(out), err)
	}
}
