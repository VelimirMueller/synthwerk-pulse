package usage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
)

// Retention. An answer older than KeepDays is not counted: its message id may already be
// pruned from the dedupe table, so it could count twice.
const (
	KeepDays      = 32
	keepDailyDays = 400
)

// Schema creates the usage tables. pulse_ai_usage_seen dedupes on the message id: the
// durable consumer may deliver an answer again after a restart or a failed ack.
const Schema = `
CREATE TABLE IF NOT EXISTS pulse_ai_usage_daily (
    day            date   NOT NULL,
    org_id         text   NOT NULL,
    provider       text   NOT NULL,
    model          text   NOT NULL,
    answers        bigint NOT NULL DEFAULT 0,
    input_tokens   bigint NOT NULL DEFAULT 0,
    output_tokens  bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (day, org_id, provider, model)
);
CREATE TABLE IF NOT EXISTS pulse_ai_usage_seen (
    message_id  text PRIMARY KEY,
    day         date NOT NULL
);
CREATE INDEX IF NOT EXISTS pulse_ai_usage_seen_day ON pulse_ai_usage_seen (day);`

// ErrBadEvent is an event that can never be counted. The consumer terminates it.
var ErrBadEvent = errors.New("usage: event without org id, time, message id or valid numbers")

// Store counts the answers per day and reads the cost. Safe for concurrent use.
type Store struct {
	db     *pgxpool.Pool
	prices Prices
	now    func() time.Time
}

// NewStore returns a store. now nil: time.Now.
func NewStore(db *pgxpool.Pool, prices Prices, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{db: db, prices: prices, now: now}
}

// Migrate creates the usage tables.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("usage: migrate: %w", err)
	}
	return nil
}

type event struct {
	Time  time.Time `json:"time"`
	OrgID string    `json:"orgid"`
	Data  struct {
		MessageID    string `json:"message_id"`
		Model        string `json:"model"`
		Provider     string `json:"provider"`
		InputTokens  int64  `json:"input_tokens"`
		OutputTokens int64  `json:"output_tokens"`
	} `json:"data"`
}

// Answer is the part of one llm answer that the AI cost counts.
type Answer struct {
	MessageID    string
	Day          time.Time // start of the UTC day
	OrgID        string
	Provider     string
	Model        string
	InputTokens  int64
	OutputTokens int64
}

// ParseAnswer reads one llm.message.completed.v1 CloudEvent. It returns ErrBadEvent
// when the event can never be counted.
func ParseAnswer(raw []byte) (Answer, error) {
	var e event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Answer{}, ErrBadEvent
	}
	d := e.Data
	if e.OrgID == "" || len(e.OrgID) > 64 || e.Time.IsZero() || d.MessageID == "" || len(d.MessageID) > 64 ||
		!providerRE.MatchString(d.Provider) || d.Model == "*" || !modelRE.MatchString(d.Model) ||
		d.InputTokens < 0 || d.OutputTokens < 0 || d.InputTokens > 10_000_000 || d.OutputTokens > 10_000_000 {
		return Answer{}, ErrBadEvent
	}
	return Answer{
		MessageID: d.MessageID, Day: today(e.Time), OrgID: e.OrgID, Provider: d.Provider, Model: d.Model,
		InputTokens: d.InputTokens, OutputTokens: d.OutputTokens,
	}, nil
}

// Record counts one llm.message.completed.v1 CloudEvent once. It reports whether the
// answer was new. Every end state counts: a failed or canceled answer may have used tokens.
func (s *Store) Record(ctx context.Context, raw []byte) (bool, error) {
	a, err := ParseAnswer(raw)
	if err != nil {
		return false, err
	}
	if a.Day.Before(today(s.now()).AddDate(0, 0, -KeepDays)) {
		return false, nil // too old to dedupe safely
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("usage: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `INSERT INTO pulse_ai_usage_seen (message_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING`, a.MessageID, a.Day)
	if err != nil {
		return false, fmt.Errorf("usage: dedupe: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO pulse_ai_usage_daily (day, org_id, provider, model, answers, input_tokens, output_tokens)
		VALUES ($1, $2, $3, $4, 1, $5, $6)
		ON CONFLICT (day, org_id, provider, model) DO UPDATE
		SET answers = pulse_ai_usage_daily.answers + 1,
		    input_tokens = pulse_ai_usage_daily.input_tokens + EXCLUDED.input_tokens,
		    output_tokens = pulse_ai_usage_daily.output_tokens + EXCLUDED.output_tokens`,
		a.Day, a.OrgID, a.Provider, a.Model, a.InputTokens, a.OutputTokens); err != nil {
		return false, fmt.Errorf("usage: count: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("usage: commit: %w", err)
	}
	return true, nil
}

// Prune deletes dedupe rows older than KeepDays and day rows older than 400 days.
func (s *Store) Prune(ctx context.Context) error {
	t := today(s.now())
	if _, err := s.db.Exec(ctx, `DELETE FROM pulse_ai_usage_seen WHERE day < $1`, t.AddDate(0, 0, -KeepDays)); err != nil {
		return fmt.Errorf("usage: prune: %w", err)
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM pulse_ai_usage_daily WHERE day < $1`, t.AddDate(0, 0, -keepDailyDays)); err != nil {
		return fmt.Errorf("usage: prune: %w", err)
	}
	return nil
}

// Usage is the counted answers of one UTC day, provider and model alias.
type Usage struct {
	Day                                time.Time
	Provider, Model                    string
	Answers, InputTokens, OutputTokens int64
}

// Cost returns the usage and cost of the last metrics.CostDays UTC days of org, or of
// every org when org is empty. It implements metrics.CostSource.
func (s *Store) Cost(ctx context.Context, org string) (metrics.AICost, error) {
	t := today(s.now())
	from := t.AddDate(0, 0, -(metrics.CostDays - 1))
	rows, err := s.db.Query(ctx, `
		SELECT day, provider, model, sum(answers)::bigint, sum(input_tokens)::bigint, sum(output_tokens)::bigint
		FROM pulse_ai_usage_daily
		WHERE day >= $1 AND day <= $2 AND ($3 = '' OR org_id = $3)
		GROUP BY day, provider, model`, from, t, org)
	if err != nil {
		return metrics.AICost{}, fmt.Errorf("usage: cost: %w", err)
	}
	defer rows.Close()
	var list []Usage
	for rows.Next() {
		var u Usage
		if err := rows.Scan(&u.Day, &u.Provider, &u.Model, &u.Answers, &u.InputTokens, &u.OutputTokens); err != nil {
			return metrics.AICost{}, fmt.Errorf("usage: cost: %w", err)
		}
		list = append(list, u)
	}
	if err := rows.Err(); err != nil {
		return metrics.AICost{}, fmt.Errorf("usage: cost: %w", err)
	}
	return BuildCost(s.prices, t, list), nil
}

// BuildCost sums list into the last metrics.CostDays UTC days up to now, priced with
// prices. Usage outside these days is left out. Each cost is rounded to 6 decimals.
func BuildCost(prices Prices, now time.Time, list []Usage) metrics.AICost {
	from := today(now).AddDate(0, 0, -(metrics.CostDays - 1))
	out := metrics.AICost{Currency: prices.Currency, Days: make([]metrics.CostDay, metrics.CostDays)}
	for i := range out.Days {
		out.Days[i].Date = from.AddDate(0, 0, i).Format(time.DateOnly)
	}
	for _, u := range list {
		d := today(u.Day)
		if d.Before(from) {
			continue
		}
		i := int(d.Sub(from).Hours() / 24)
		if i >= len(out.Days) {
			continue
		}
		cd := &out.Days[i]
		cd.Answers += u.Answers
		cd.InputTokens += u.InputTokens
		cd.OutputTokens += u.OutputTokens
		if price, ok := prices.Lookup(u.Provider, u.Model); ok {
			cd.Cost += price.Cost(u.InputTokens, u.OutputTokens)
		} else {
			cd.UnpricedAnswers += u.Answers
		}
	}
	for i := range out.Days {
		out.Days[i].Cost = math.Round(out.Days[i].Cost*1e6) / 1e6
	}
	return out
}

// today is the start of the UTC day of t.
func today(t time.Time) time.Time { return t.UTC().Truncate(24 * time.Hour) }
