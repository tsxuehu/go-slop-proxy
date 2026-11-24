package server

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// maxBodyBytes matches the Rust original's 192 MiB ceiling. Codex zstd-encodes
// large request bodies, and a 200k-token context runs well past axum's 2MB
// default even after compression.
const maxBodyBytes = 192 * 1024 * 1024

var (
	errZstdMalformed = errors.New("malformed zstd request body")
	errZstdTooLarge  = errors.New("request body too large")
)

// decompressMiddleware caps every request body and inflates zstd-encoded ones
// back into plain JSON before the router sees them. Other encodings pass
// through untouched (the original only special-cases zstd).
func decompressMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		if !strings.EqualFold(r.Header.Get("Content-Encoding"), "zstd") {
			next.ServeHTTP(w, r)
			return
		}
		encoded, err := io.ReadAll(r.Body)
		if err != nil {
			writeAPIError(w, dialectOpenAI, http.StatusRequestEntityTooLarge,
				"invalid_request_error", "request body too large")
			return
		}
		plain, err := decodeZstd(encoded, maxBodyBytes)
		switch {
		case errors.Is(err, errZstdTooLarge):
			writeAPIError(w, dialectOpenAI, http.StatusRequestEntityTooLarge,
				"invalid_request_error", "request body too large")
			return
		case err != nil:
			writeAPIError(w, dialectOpenAI, http.StatusBadRequest,
				"invalid_request_error", "malformed zstd request body")
			return
		}
		r.Header.Del("Content-Encoding")
		r.Header.Set("Content-Length", strconv.Itoa(len(plain)))
		r.ContentLength = int64(len(plain))
		r.Body = io.NopCloser(bytes.NewReader(plain))
		next.ServeHTTP(w, r)
	})
}

// decodeZstd reads one byte past the limit so a body that would exceed it is
// refused outright rather than truncated into a half JSON document.
func decodeZstd(encoded []byte, limit int) ([]byte, error) {
	zr, err := zstd.NewReader(bytes.NewReader(encoded))
	if err != nil {
		return nil, errZstdMalformed
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, int64(limit)+1))
	if err != nil {
		return nil, errZstdMalformed
	}
	if len(out) > limit {
		return nil, errZstdTooLarge
	}
	return out, nil
}
