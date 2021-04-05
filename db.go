package divyield

import (
    "time"
    "fmt"
    "context"
)

type DB interface {
    InitSchema(ctx context.Context, tickers []string) error

	Prices(ctx context.Context, ticker string, f *PriceFilter) ([]*Price, error)
	PrependPrices(ctx context.Context, ticker string, prices []*Price) error

	Dividends(ctx context.Context, ticker string, f *DividendFilter) ([]*Dividend, error)
	PrependDividends(ctx context.Context, ticker string, dividends []*Dividend) error
}

type Price struct {
	Date    time.Time
	Symbol  string
	Close   float64
	High    float64
	Low     float64
	Open    float64
	Volume  float64
}

const DateFormat = "2006-01-02"

func (p *Price) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(p.Date).Format(DateFormat),
		p.Close,
	)
}

type PriceFilter struct {
	From time.Time
    Limit uint64
}


type Dividend struct {
	ExDate       time.Time
	Amount       float64
	Currency     string
	Frequency    int
	Symbol       string
}

func (d *Dividend) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(d.ExDate).Format(DateFormat),
		d.Amount,
	)
}

type DividendFilter struct {
	From time.Time
    Limit uint64
}
