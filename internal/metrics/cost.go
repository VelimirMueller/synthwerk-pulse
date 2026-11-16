package metrics

import "context"

// CostDays is the number of UTC days in the AI cost: today and the 6 days before.
const CostDays = 7

// AICost is the AI cost per UTC day of one scope (all orgs or one org).
type AICost struct {
	// Currency of every cost, for example USD (the price table of pulse).
	Currency string `json:"currency"`
	// Days are the last CostDays UTC days, oldest first, today last. Days without an
	// answer are in the list with zeros.
	Days []CostDay `json:"days"`
}

// CostDay is the AI usage and cost of one UTC day.
type CostDay struct {
	// Date is the UTC day, YYYY-MM-DD.
	Date string `json:"date"`
	// Cost of the priced answers, in the currency.
	Cost         float64 `json:"cost"`
	Answers      int64   `json:"answers"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	// UnpricedAnswers have no price in the table. They add nothing to Cost.
	UnpricedAnswers int64 `json:"unpriced_answers"`
}

// CostSource gives the AI cost per day. org "" is every org.
type CostSource interface {
	Cost(ctx context.Context, org string) (AICost, error)
}
