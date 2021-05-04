package stats

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"szakszon.com/divyield"
	"szakszon.com/divyield/logger"
)

type options struct {
	stocksDir        string
	now              time.Time
	logger           logger.Logger
	db               divyield.DB
	dividendYieldMin float64
	dividendYieldMax float64
}

type Option func(o options) options

func StocksDir(dir string) Option {
	return func(o options) options {
		o.stocksDir = dir
		return o
	}
}

func Now(n time.Time) Option {
	return func(o options) options {
		o.now = n
		return o
	}
}

func Log(l logger.Logger) Option {
	return func(o options) options {
		o.logger = l
		return o
	}
}

func DB(db divyield.DB) Option {
	return func(o options) options {
		o.db = db
		return o
	}
}

func DividendYieldMin(v float64) Option {
	return func(o options) options {
		o.dividendYieldMin = v
		return o
	}
}

func DividendYieldMax(v float64) Option {
	return func(o options) options {
		o.dividendYieldMax = v
		return o
	}
}

var defaultOptions = options{
	logger: nil,
}

func NewStatsGenerator(os ...Option) StatsGenerator {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}
	return StatsGenerator{
		opts: opts,
	}
}

type StatsGenerator struct {
	opts options
}

type StatsRow struct {
	Ticker               string
	ForwardDividendYield float64
	ForwardDividend      float64
	DividendChangeMR     *DividendChangeMR
	DGR1y                float64
	DGR3y                float64
	DGR5y                float64
	DGR10y               float64
	DividendsAnnual      []*DividendAnnual
}

func (r *StatsRow) DGR(n int) float64 {
	sum := float64(0)
	for _, a := range r.DividendsAnnual[0:n] {
		sum += a.ChangeRate
	}
	return sum / float64(n)
}

type DividendChangeMR struct {
	ChangePercent float64
	Date          time.Time
}

type DividendAnnual struct {
	Year          int
	Amount        float64
	PayoutPerYear int
	ChangeRate    float64 // compared to the year before
}

type Stats struct {
	Rows             []*StatsRow
	DividendYieldMin float64
	DividendYieldMax float64
}

func (s *Stats) String() string {
	out := &bytes.Buffer{}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)

	b := &bytes.Buffer{}
	b.WriteString("Ticker")
	b.WriteByte('\t')
	b.WriteString("Forward Yield")
	b.WriteByte('\t')
	b.WriteString("Forward Dividend")
	b.WriteByte('\t')
	b.WriteString("MR%")
	b.WriteByte('\t')
	b.WriteString("MR% Ex-Div Date")
	b.WriteByte('\t')
	b.WriteString("DGR-1y")
	b.WriteByte('\t')
	b.WriteString("DGR-3y")
	b.WriteByte('\t')
	//b.WriteString("DGR-5y")
	//b.WriteByte('\t')
	//b.WriteString("DGR-10y")
	//b.WriteByte('\t')

	// if len(s.Rows) > 0 {
	// 	for _, d := range s.Rows[0].DividendsAnnual {
	// 		b.WriteString("DGR-" + strconv.Itoa(d.Year) + " (DPS)")
	// 		b.WriteByte('\t')
	// 	}
	// }
	fmt.Fprintln(w, b.String())

	for _, row := range s.Rows {
		y := row.ForwardDividendYield
		if s.DividendYieldMin > 0 &&
			(math.IsNaN(y) || math.IsInf(y, 1) || math.IsInf(y, -1) || s.DividendYieldMin > y) {
			continue
		}
		if s.DividendYieldMax > 0 &&
			(math.IsNaN(y) || math.IsInf(y, 1) || math.IsInf(y, -1) || s.DividendYieldMax < y) {
			continue
		}

		b.Reset()
		b.WriteString(fmt.Sprintf("%-6v", row.Ticker))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.ForwardDividendYield))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f", row.ForwardDividend))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DividendChangeMR.ChangePercent))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%s", row.DividendChangeMR.Date.Format("2006-01-02")))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(1)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(3)))
		b.WriteByte('\t')
		//b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(5)))
		//b.WriteByte('\t')
		//b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(10)))
		//b.WriteByte('\t')

		// for _, d := range row.DividendsAnnual {
		// 	b.WriteString(fmt.Sprintf("%.2f%% (%.2f)", d.ChangeRate, d.Amount))
		// 	b.WriteByte('\t')
		// }

		fmt.Fprintln(w, b.String())
	}

	fmt.Fprintln(w, "")

	if s.DividendYieldMin > 0 {
		fmt.Fprintln(w, "Min dividend yield:",
			strconv.FormatFloat(s.DividendYieldMin, 'f', 2, 64)+"%")
	}
	if s.DividendYieldMax > 0 {
		fmt.Fprintln(w, "Max dividend yield:",
			strconv.FormatFloat(s.DividendYieldMax, 'f', 2, 64)+"%")
	}

	w.Flush()
	return out.String()
}

type result struct {
	Ticker string
	Row    *StatsRow
	Err    error
}

type StatsError struct {
	Ticker string
	Err    error
}

func (e *StatsError) Error() string {
	return fmt.Sprintf("%s: %s", e.Ticker, e.Err)
}

func (f *StatsGenerator) Generate(ctx context.Context, tickers []string) (*Stats, error) {
	var workerWg sync.WaitGroup
	var resultWg sync.WaitGroup
	resultCh := make(chan result)

	stats := &Stats{
		DividendYieldMin: f.opts.dividendYieldMin,
		DividendYieldMax: f.opts.dividendYieldMax,
	}

	errs := make([]error, 0)

	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for res := range resultCh {
			if res.Err != nil {
				se := &StatsError{Ticker: res.Ticker, Err: res.Err}
				errs = append(errs, se)
			} else {
				stats.Rows = append(stats.Rows, res.Row)
			}
		}

		sort.SliceStable(stats.Rows, func(i, j int) bool {
			return stats.Rows[i].Ticker < stats.Rows[j].Ticker
		})
	}()

LOOP:
	for _, ticker := range tickers {
		ticker := ticker

		select {
		case <-ctx.Done():
			break LOOP
		default:
			// noop
		}

		workerWg.Add(1)
		go func(ticker string) {
			defer workerWg.Done()
			row, err := f.generateStatsRow(ctx, ticker)
			resultCh <- result{Ticker: ticker, Row: row, Err: err}
		}(ticker)
	}

	workerWg.Wait()
	close(resultCh)
	resultWg.Wait()

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return stats, nil
}

func (f *StatsGenerator) generateStatsRow(
	ctx context.Context,
	ticker string,
) (*StatsRow, error) {

	dyf := &divyield.DividendYieldFilter{
		Limit: 1,
	}
	dividendYields, err := f.opts.db.DividendYields(ctx, ticker, dyf)
	if err != nil {
		return nil, fmt.Errorf("get dividend yields: %s", err)
	}

	forwardDivYield := float64(0)
	forwardDiv := float64(0)
	if len(dividendYields) > 0 {
		forwardDivYield = dividendYields[0].ForwardTTM()
		forwardDiv = dividendYields[0].ForwardDividend()
	}

	from := time.Date(
		f.opts.now.UTC().Year()-11, time.January, 1,
		1, 0, 0, 0, time.UTC)
	df := &divyield.DividendFilter{
		From:     from,
		CashOnly: true,
		Regular:  true,
	}
	dividends, err := f.opts.db.Dividends(ctx, ticker, df)
	if err != nil {
		return nil, fmt.Errorf("get dividends: %s", err)
	}

	mr, err := f.dividendChangeMostRecent(dividends)
	if err != nil {
		return nil, fmt.Errorf("most recent dividend change: %s", err)
	}

	divsAnnual, err := f.dividendsAnnual(dividends)
	if err != nil {
		return nil, fmt.Errorf("dividends annual: %s", err)
	}

	row := &StatsRow{
		Ticker:               ticker,
		ForwardDividendYield: forwardDivYield,
		ForwardDividend:      forwardDiv,
		DividendChangeMR:     mr,
		DividendsAnnual:      divsAnnual,
	}

	return row, nil
}

func (f *StatsGenerator) dividendChangeMostRecent(
	dividends []*divyield.Dividend,
) (*DividendChangeMR, error) {
	mr := &DividendChangeMR{
		ChangePercent: float64(0),
		Date:          time.Time{},
	}

	if len(dividends) < 2 {
		return mr, nil
	}

	for i := 0; i <= len(dividends)-2; i++ {
		d1 := dividends[i]
		d0 := dividends[i+1]

		if (d1.AmountNorm() - d0.AmountNorm()) != 0 {
			changePercent := math.NaN()
			if d1.Currency == "USD" && d0.Currency == "USD" {
				changePercent = ((d1.AmountNorm() - d0.AmountNorm()) /
					d0.AmountNorm()) * 100
			}

			mr = &DividendChangeMR{
				ChangePercent: changePercent,
				Date:          d1.ExDate,
			}
			break
		}
	}

	return mr, nil
}

func (f *StatsGenerator) dividendsAnnual(
	dividends []*divyield.Dividend,
) ([]*DividendAnnual, error) {
	divsAnnualMap := map[int]*DividendAnnual{}

	endYear := f.opts.now.Year() - 1
	startYear := f.opts.now.Year() - 12

	for i := startYear; i <= endYear; i++ {
		divsAnnualMap[i] = &DividendAnnual{Year: i}
	}

	for _, d := range dividends {
		if d.ExDate.Year() < startYear {
			break
		}

		if d.ExDate.Year() > endYear {
			continue
		}

		a := divsAnnualMap[d.ExDate.Year()]
		a.Amount += d.Amount
		a.PayoutPerYear += 1
	}

	divsAnnual := []*DividendAnnual{}
	for _, a := range divsAnnualMap {
		divsAnnual = append(divsAnnual, a)
	}

	sort.SliceStable(divsAnnual, func(i, j int) bool {
		return divsAnnual[i].Year > divsAnnual[j].Year
	})

	for i := 0; i <= len(divsAnnual)-2; i++ {
		a1 := divsAnnual[i]
		a0 := divsAnnual[i+1]
		a1.ChangeRate = ((a1.Amount - a0.Amount) / a0.Amount) * 100
	}

	return divsAnnual, nil
}

type Dividend struct {
	Date   time.Time
	Amount float64
}

func (f *StatsGenerator) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}
