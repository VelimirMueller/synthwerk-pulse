// Price table of the AI cost (synthwerk#57).
package usage_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VelimirMueller/synthwerk-pulse/internal/usage"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prices.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPrices(t *testing.T) {
	t.Run("LoadPrices - exact alias and wildcard - exact alias first", func(t *testing.T) {
		p, err := usage.LoadPrices(write(t, `
currency: EUR
prices:
  - {provider: anthropic, model: default, input_per_million: 3, output_per_million: 15}
  - {provider: anthropic, model: "*", input_per_million: 1, output_per_million: 5}
`))
		if err != nil {
			t.Fatal(err)
		}
		if p.Currency != "EUR" {
			t.Fatalf("currency %q", p.Currency)
		}
		e, ok := p.Lookup("anthropic", "default")
		if !ok || e.Cost(1_000_000, 100_000) != 4.5 {
			t.Fatalf("default: %+v %v, want 3 + 1.5", e, ok)
		}
		if e, ok := p.Lookup("anthropic", "fast"); !ok || e.Cost(1_000_000, 0) != 1 {
			t.Fatalf("fast: %+v %v, want the wildcard price", e, ok)
		}
		if _, ok := p.Lookup("openai_compat", "default"); ok {
			t.Fatal("openai_compat has a price")
		}
	})

	t.Run("DefaultPrices - local providers free, others unpriced", func(t *testing.T) {
		p := usage.DefaultPrices()
		if e, ok := p.Lookup("ollama", "default"); !ok || e.Cost(1e6, 1e6) != 0 || p.Currency != "USD" {
			t.Fatalf("ollama %+v %v", e, ok)
		}
		if _, ok := p.Lookup("anthropic", "default"); ok {
			t.Fatal("anthropic has a default price")
		}
	})

	for name, body := range map[string]string{
		"unknown field":     "prices:\n  - {provider: a, model: b, input_per_million: 1, output_per_million: 1, extra: 1}\n",
		"negative price":    "prices:\n  - {provider: a, model: b, input_per_million: -1, output_per_million: 1}\n",
		"listed twice":      "prices:\n  - {provider: a, model: b}\n  - {provider: a, model: b}\n",
		"bad currency":      "currency: dollars\n",
		"bad provider name": "prices:\n  - {provider: A-B, model: b}\n",
	} {
		t.Run("LoadPrices - "+name+" - error", func(t *testing.T) {
			if _, err := usage.LoadPrices(write(t, body)); err == nil {
				t.Fatal("accepted")
			}
		})
	}

	t.Run("LoadPrices - empty file - USD without prices", func(t *testing.T) {
		p, err := usage.LoadPrices(write(t, ""))
		if err != nil || p.Currency != "USD" {
			t.Fatalf("%+v %v", p, err)
		}
	})

	t.Run("LoadPrices - file over 1 MiB - error", func(t *testing.T) {
		if _, err := usage.LoadPrices(write(t, "# "+strings.Repeat("x", 1<<20))); err == nil {
			t.Fatal("accepted")
		}
	})
}
