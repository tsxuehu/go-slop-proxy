package translate

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"
)

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Deterministic fallback is acceptable for IDs; never affects routing.
		return strings.Repeat("0", n*2)
	}
	return hex.EncodeToString(b)
}

// ParseSSE reads an SSE stream and yields each event's type + raw data.
// It skips comments and [DONE] markers.
func ParseSSE(r io.Reader, handle func(eventType, data string) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var event, data string
	flush := func() error {
		if data == "" {
			return nil
		}
		d := strings.TrimSuffix(data, "\n")
		if d == "[DONE]" {
			return nil
		}
		return handle(event, d)
	}
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			event, data = "", ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data += strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ") + "\n"
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return scanner.Err()
}
