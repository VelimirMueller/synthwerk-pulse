// Package target reads the list of targets pulse checks (targets.yaml, design §7).
//
// Targets come only from this file, at start. There is no API to add one, so a caller
// cannot make pulse send requests to an address of its choice (design §9, SSRF).
//
//	targets:
//	  - name: bus
//	    kind: http
//	    url: http://bus:8222/healthz?js-enabled-only=true
//	    interval: 5s
//	  - name: database
//	    kind: tcp
//	    address: database:5432
package target

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"go.yaml.in/yaml/v3"
)

// Kind is how pulse checks a target.
type Kind string

// The kinds.
const (
	// HTTP sends GET to URL. A /readyz body (openapi/_shared/health.yaml) gives the
	// status and the checks. Any other 2xx or 3xx answer is ok.
	HTTP Kind = "http"
	// TCP opens a TCP connection to Address. For a part without a health endpoint.
	TCP Kind = "tcp"
)

// Limits.
const (
	MaxTargets  = 64
	MinInterval = time.Second
	MaxInterval = 5 * time.Minute
	MaxTimeout  = 10 * time.Second
	maxFileSize = 1 << 20
)

// Defaults apply to a target that sets no interval or timeout.
type Defaults struct {
	Interval time.Duration
	Timeout  time.Duration
}

// DefaultDefaults: poll every 15 s, time out after 3 s (design §3.3 and §8).
var DefaultDefaults = Defaults{Interval: 15 * time.Second, Timeout: 3 * time.Second}

// Target is one checked service or infra part.
type Target struct {
	// Name matches ^[a-z][a-z0-9-]{0,63}$. It is the target of the status event.
	Name     string        `yaml:"name"`
	Kind     Kind          `yaml:"kind"`
	URL      string        `yaml:"url"`
	Address  string        `yaml:"address"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

type file struct {
	Targets []Target `yaml:"targets"`
}

var nameRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// Load reads and checks the targets file.
func Load(path string, d Defaults) ([]Target, error) {
	f, err := os.Open(path) //nolint:gosec // the path comes from the operator (env), not from a request.
	if err != nil {
		return nil, fmt.Errorf("target: %w", err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("target: read %s: %w", path, err)
	}
	if len(data) > maxFileSize {
		return nil, fmt.Errorf("target: %s is larger than %d bytes", path, maxFileSize)
	}
	targets, err := Parse(data, d)
	if err != nil {
		return nil, fmt.Errorf("target: %s: %w", path, err)
	}
	return targets, nil
}

// Parse decodes a targets file, fills the defaults and checks every target.
// Unknown keys are an error. All problems are returned together.
func Parse(data []byte, d Defaults) ([]Target, error) {
	var f file
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(f.Targets) > MaxTargets {
		return nil, fmt.Errorf("%d targets, max %d", len(f.Targets), MaxTargets)
	}

	var errs []error
	seen := map[string]bool{}
	for i := range f.Targets {
		t := &f.Targets[i]
		if t.Interval == 0 {
			t.Interval = d.Interval
		}
		if t.Timeout == 0 {
			t.Timeout = d.Timeout
		}
		if err := t.validate(); err != nil {
			errs = append(errs, fmt.Errorf("targets[%d] %q: %w", i, t.Name, err))
		}
		if seen[t.Name] {
			errs = append(errs, fmt.Errorf("targets[%d]: name %q is used twice", i, t.Name))
		}
		seen[t.Name] = true
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return f.Targets, nil
}

func (t Target) validate() error {
	var errs []error
	if !nameRe.MatchString(t.Name) {
		errs = append(errs, errors.New("name must match ^[a-z][a-z0-9-]{0,63}$"))
	}
	switch t.Kind {
	case HTTP:
		if t.Address != "" {
			errs = append(errs, errors.New("kind http takes url, not address"))
		}
		if err := checkURL(t.URL); err != nil {
			errs = append(errs, err)
		}
	case TCP:
		if t.URL != "" {
			errs = append(errs, errors.New("kind tcp takes address, not url"))
		}
		if err := checkAddress(t.Address); err != nil {
			errs = append(errs, err)
		}
	default:
		errs = append(errs, fmt.Errorf("kind %q is not http or tcp", t.Kind))
	}
	if t.Interval < MinInterval || t.Interval > MaxInterval {
		errs = append(errs, fmt.Errorf("interval %s is not between %s and %s", t.Interval, MinInterval, MaxInterval))
	}
	if t.Timeout <= 0 || t.Timeout > MaxTimeout || t.Timeout > t.Interval {
		errs = append(errs, fmt.Errorf("timeout %s must be above 0, at most %s and at most the interval", t.Timeout, MaxTimeout))
	}
	return errors.Join(errs...)
}

func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("url does not parse")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("url must start with http:// or https://")
	}
	if u.Hostname() == "" {
		return errors.New("url has no host")
	}
	if u.User != nil {
		return errors.New("url must not hold a user or password")
	}
	return nil
}

func checkAddress(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return errors.New("address must be host:port")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return errors.New("address port must be 1 to 65535")
	}
	return nil
}
