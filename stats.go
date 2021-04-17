package divyield

import (
	"context"
	"time"
)

type StatsGenerator interface {
	Generate(ctx context.Context, tickers []string) (*Stats, error)
}

type Stats struct {
	Rows []*StatsRow
}

type StatsRow struct {
	Ticker           string
	DivYield         float64
	DividendChangeMR *DividendChangeMR
	DGR1y            float64
	DGR3y            float64
	DGR5y            float64
	DGR10y           float64
	DividendsAnnual  []*DividendAnnual
}

func (r *StatsRow) DGR(n int) float64 {
	sum := float64(0)
	for _, a := range r.DividendsAnnual[0:n] {
		sum += a.ChangeRate
	}
	return sum / float64(n)
}

type DividendChangeMR struct {
	Amount float64
	Date   time.Time
}

type DividendAnnual struct {
	Year          int
	Amount        float64
	PayoutPerYear int
	ChangeRate    float64 // compared to the year before
}
