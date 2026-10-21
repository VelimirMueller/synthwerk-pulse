package target_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
)

var defaults = target.Defaults{Interval: 15 * time.Second, Timeout: 3 * time.Second}

func TestParse(t *testing.T) {
	t.Run("Parse - valid file - targets with defaults filled", func(t *testing.T) {
		got, err := target.Parse([]byte(`
targets:
  - name: bus
    kind: http
    url: http://bus:8222/healthz?js-enabled-only=true
    interval: 5s
  - name: database
    kind: tcp
    address: database:5432
    timeout: 1s
`), defaults)
		if err != nil {
			t.Fatal(err)
		}
		want := []target.Target{
			{Name: "bus", Kind: target.HTTP, URL: "http://bus:8222/healthz?js-enabled-only=true", Interval: 5 * time.Second, Timeout: 3 * time.Second},
			{Name: "database", Kind: target.TCP, Address: "database:5432", Interval: 15 * time.Second, Timeout: time.Second},
		}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("Parse - empty file - no targets", func(t *testing.T) {
		got, err := target.Parse(nil, defaults)
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v; want no targets, nil", got, err)
		}
	})

	bad := []struct {
		name, yaml, wantErr string
	}{
		{"Parse - unknown key - error", "targets:\n  - name: a\n    kind: tcp\n    address: a:1\n    port: 1\n", "field port not found"},
		{"Parse - bad name - error", "targets:\n  - name: Bus\n    kind: tcp\n    address: a:1\n", "name must match"},
		{"Parse - name used twice - error", "targets:\n  - {name: a, kind: tcp, address: a:1}\n  - {name: a, kind: tcp, address: b:1}\n", "used twice"},
		{"Parse - unknown kind - error", "targets:\n  - {name: a, kind: grpc, address: a:1}\n", "is not http or tcp"},
		{"Parse - file url - error", "targets:\n  - {name: a, kind: http, url: 'file:///etc/passwd'}\n", "must start with http"},
		{"Parse - url with password - error", "targets:\n  - {name: a, kind: http, url: 'http://u:p@a/readyz'}\n", "must not hold a user"},
		{"Parse - tcp without port - error", "targets:\n  - {name: a, kind: tcp, address: database}\n", "host:port"},
		{"Parse - http with address - error", "targets:\n  - {name: a, kind: http, url: 'http://a/', address: a:1}\n", "takes url, not address"},
		{"Parse - interval below 1s - error", "targets:\n  - {name: a, kind: tcp, address: a:1, interval: 100ms, timeout: 50ms}\n", "interval 100ms"},
		{"Parse - timeout above interval - error", "targets:\n  - {name: a, kind: tcp, address: a:1, interval: 2s, timeout: 3s}\n", "timeout 3s"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			_, err := target.Parse([]byte(c.yaml), defaults)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error %v, want one with %q", err, c.wantErr)
			}
		})
	}

	t.Run("Parse - 65 targets - error", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("targets:\n")
		for i := range 65 {
			b.WriteString("  - {name: t" + string(rune('a'+i%26)) + strings.Repeat("x", i/26) + ", kind: tcp, address: a:1}\n")
		}
		if _, err := target.Parse([]byte(b.String()), defaults); err == nil || !strings.Contains(err.Error(), "max 64") {
			t.Fatalf("error %v, want max 64", err)
		}
	})
}

func TestLoad(t *testing.T) {
	t.Run("Load - repo example file - valid", func(t *testing.T) {
		got, err := target.Load(filepath.Join("..", "..", "..", "targets.example.yaml"), defaults)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			t.Fatal("targets.example.yaml lists no target")
		}
	})

	t.Run("Load - missing file - error names the path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nope.yaml")
		if _, err := target.Load(path, defaults); err == nil || !strings.Contains(err.Error(), "nope.yaml") {
			t.Fatalf("error %v, want one with the path", err)
		}
	})

	t.Run("Load - bad file - error names the path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.yaml")
		if err := os.WriteFile(path, []byte("targets:\n  - {name: A, kind: tcp, address: a:1}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := target.Load(path, defaults); err == nil || !strings.Contains(err.Error(), "bad.yaml") {
			t.Fatalf("error %v, want one with the path", err)
		}
	})
}
