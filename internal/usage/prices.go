// Package usage counts the AI tokens per day and gives the AI cost per day
// (synthwerk#57, docs/features/alerts-and-cost.md).
//
// A durable consumer reads llm.message.completed.v1 from EVT_LLM. Each answer counts
// once (dedupe on message_id) per UTC day, org, provider and model alias in Postgres.
// The cost comes from the price table when the numbers are read, so a corrected price
// also corrects the past days.
package usage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"

	"go.yaml.in/yaml/v3"
)

// Limits of the price file.
const (
	maxPrices       = 256
	maxPriceFile    = 1 << 20
	maxPerMillion   = 10_000.0
	defaultCurrency = "USD"
)

var (
	currencyRE = regexp.MustCompile(`^[A-Z]{3}$`)
	providerRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	modelRE    = regexp.MustCompile(`^([a-z][a-z0-9_-]{0,63}|\*)$`)
)

// Price is the price of one provider and model alias, in the currency of the table,
// per one million tokens. Model "*" is every alias of the provider.
type Price struct {
	Provider         string  `yaml:"provider"`
	Model            string  `yaml:"model"`
	InputPerMillion  float64 `yaml:"input_per_million"`
	OutputPerMillion float64 `yaml:"output_per_million"`
}

// Prices is the price table. The zero value has no prices.
type Prices struct {
	Currency string
	byKey    map[[2]string]Price
}

// DefaultPrices is the table without a price file: USD, and the local providers
// ollama and fake cost nothing. Every other provider is unpriced.
func DefaultPrices() Prices {
	p, err := newPrices(defaultCurrency, []Price{
		{Provider: "ollama", Model: "*"},
		{Provider: "fake", Model: "*"},
	})
	if err != nil {
		panic(err) // the list above is fixed: an error is a programming error
	}
	return p
}

// LoadPrices reads a price file:
//
//	currency: USD
//	prices:
//	  - provider: anthropic
//	    model: default
//	    input_per_million: 3
//	    output_per_million: 15
//	  - provider: ollama
//	    model: "*"
//	    input_per_million: 0
//	    output_per_million: 0
func LoadPrices(path string) (Prices, error) {
	f, err := os.Open(path) //nolint:gosec // the path comes from the operator's env, not a request
	if err != nil {
		return Prices{}, fmt.Errorf("usage: price file: %w", err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxPriceFile+1))
	if err != nil {
		return Prices{}, fmt.Errorf("usage: price file: %w", err)
	}
	if len(raw) > maxPriceFile {
		return Prices{}, errors.New("usage: price file is larger than 1 MiB")
	}
	var file struct {
		Currency string  `yaml:"currency"`
		Prices   []Price `yaml:"prices"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&file); err != nil && !errors.Is(err, io.EOF) {
		return Prices{}, fmt.Errorf("usage: price file: %w", err)
	}
	if file.Currency == "" {
		file.Currency = defaultCurrency
	}
	return newPrices(file.Currency, file.Prices)
}

func newPrices(currency string, list []Price) (Prices, error) {
	if !currencyRE.MatchString(currency) {
		return Prices{}, fmt.Errorf("usage: currency %q is not a three-letter code such as USD", currency)
	}
	if len(list) > maxPrices {
		return Prices{}, fmt.Errorf("usage: more than %d prices", maxPrices)
	}
	p := Prices{Currency: currency, byKey: make(map[[2]string]Price, len(list))}
	for i, e := range list {
		if !providerRE.MatchString(e.Provider) || !modelRE.MatchString(e.Model) {
			return Prices{}, fmt.Errorf("usage: price %d: provider or model is not valid", i+1)
		}
		for _, v := range []float64{e.InputPerMillion, e.OutputPerMillion} {
			if math.IsNaN(v) || v < 0 || v > maxPerMillion {
				return Prices{}, fmt.Errorf("usage: price %d: a price must be 0 to %.0f per million tokens", i+1, maxPerMillion)
			}
		}
		key := [2]string{e.Provider, e.Model}
		if _, dup := p.byKey[key]; dup {
			return Prices{}, fmt.Errorf("usage: price %d: %s/%s is listed twice", i+1, e.Provider, e.Model)
		}
		p.byKey[key] = e
	}
	return p, nil
}

// Lookup returns the price of provider and model: the exact alias first, then "*".
func (p Prices) Lookup(provider, model string) (Price, bool) {
	if e, ok := p.byKey[[2]string{provider, model}]; ok {
		return e, true
	}
	e, ok := p.byKey[[2]string{provider, "*"}]
	return e, ok
}

// Cost of input and output tokens at price e.
func (e Price) Cost(input, output int64) float64 {
	return (float64(input)*e.InputPerMillion + float64(output)*e.OutputPerMillion) / 1e6
}
