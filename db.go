package divyield

import (
	"context"
	"fmt"
	"time"
)

type DB interface {
	InitSchema(
		ctx context.Context,
		tickers []string,
	) error

	Prices(
		ctx context.Context,
		ticker string,
		f *PriceFilter,
	) ([]*Price, error)

	PrependPrices(
		ctx context.Context,
		ticker string,
		prices []*Price,
	) error

	Dividends(
		ctx context.Context,
		ticker string,
		f *DividendFilter,
	) ([]*Dividend, error)

	PrependDividends(
		ctx context.Context,
		ticker string,
		dividends []*Dividend,
	) error

	DividendYields(
		ctx context.Context,
		ticker string,
		f *DividendYieldFilter,
	) ([]*DividendYield, error)

	Splits(
		ctx context.Context,
		ticker string,
		f *SplitFilter,
	) ([]*Split, error)

	PrependSplits(
		ctx context.Context,
		ticker string,
		splits []*Split,
	) error
}

type Price struct {
	Date     time.Time
	Symbol   string
	Close    float64
	CloseAdj float64
	High     float64
	Low      float64
	Open     float64
	Volume   float64
}

const DateFormat = "2006-01-02"

func (p *Price) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(p.Date).Format(DateFormat),
		p.Close,
	)
}

type PriceFilter struct {
	From  time.Time
	Limit uint64
}

type Dividend struct {
	ID          int64
	ExDate      time.Time
	Amount      float64
	AmountAdj   float64
	Currency    string
	Frequency   int
	Symbol      string
	PaymentType string
}

func (d *Dividend) Year() int {
	return d.ExDate.Year()
}

func (d *Dividend) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(d.ExDate).Format(DateFormat),
		d.AmountAdj,
	)
}

func (d *Dividend) AmountNorm() float64 {
	return d.AmountAdj * float64(d.Frequency)
}

type DividendFilter struct {
	From     time.Time
	Limit    uint64
	CashOnly bool
	Regular  bool
}

type DividendYield struct {
	Date                   time.Time
	CloseAdj               float64
	DividendAdj            float64
	Frequency              int
	DividendAdjTrailingTTM float64
}

func (y *DividendYield) DividendForwardTTM() float64 {
	return y.DividendAdj * float64(y.Frequency)
}

func (y *DividendYield) ForwardTTM() float64 {
	if y.CloseAdj == 0 {
		return 0
	}
	return ((y.DividendAdj * float64(y.Frequency)) / y.CloseAdj) * 100
}

func (y *DividendYield) TrailingTTM() float64 {
	if y.CloseAdj == 0 {
		return 0
	}
	return (y.DividendAdjTrailingTTM / y.CloseAdj) * 100
}

type DividendYieldFilter struct {
	From  time.Time
	Limit uint64
}

type StockFetcher interface {
	Fetch(ctx context.Context, tickers []string)
}
