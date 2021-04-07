package stats

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"szakszon.com/divyield/logger"
	"szakszon.com/divyield/payout"
)

type options struct {
	stocksDir string
	now       time.Time
	logger    logger.Logger
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

type Stats struct {
	Rows []*StatsRow
}

func (s *Stats) String() string {
	out := &bytes.Buffer{}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)

	b := &bytes.Buffer{}
	b.WriteString("Ticker")
	b.WriteByte('\t')
	b.WriteString("DivYield")
	b.WriteByte('\t')
	b.WriteString("MR%")
	b.WriteByte('\t')
	b.WriteString("MR% Ex-Div Date")
	b.WriteByte('\t')
	b.WriteString("DGR-1y")
	b.WriteByte('\t')
	b.WriteString("DGR-3y")
	b.WriteByte('\t')
	b.WriteString("DGR-5y")
	b.WriteByte('\t')
	b.WriteString("DGR-10y")
	b.WriteByte('\t')

	// if len(s.Rows) > 0 {
	// 	for _, d := range s.Rows[0].DividendsAnnual {
	// 		b.WriteString("DGR-" + strconv.Itoa(d.Year) + " (DPS)")
	// 		b.WriteByte('\t')
	// 	}
	// }
	fmt.Fprintln(w, b.String())

	for _, row := range s.Rows {
		b.Reset()
		b.WriteString(fmt.Sprintf("%-6v", row.Ticker))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DivYield))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DividendChangeMR.Amount))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%s", row.DividendChangeMR.Date.Format("2006-01-02")))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(1)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(3)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(5)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(10)))
		b.WriteByte('\t')

		// for _, d := range row.DividendsAnnual {
		// 	b.WriteString(fmt.Sprintf("%.2f%% (%.2f)", d.ChangeRate, d.Amount))
		// 	b.WriteByte('\t')
		// }

		fmt.Fprintln(w, b.String())
	}
	w.Flush()
	return out.String()
}

type result struct {
	Row *StatsRow
	Err error
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

	stats := &Stats{}
	errs := make([]error, 0)

	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for res := range resultCh {
			if res.Err != nil {
				errs = append(errs, &StatsError{Ticker: res.Row.Ticker, Err: res.Err})
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
			row, err := f.generateStatsRow(ticker)
			resultCh <- result{Row: row, Err: err}
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

func (f *StatsGenerator) generateStatsRow(ticker string) (*StatsRow, error) {
	dividendsPath := filepath.Join(f.opts.stocksDir, ticker, "dividends.csv")
	dividends, err := parseDividends(dividendsPath)
	if err != nil {
		return nil, fmt.Errorf("parse dividends: %s: %s", dividendsPath, err)
	}

	mr, err := f.dividendChangeMostRecent(dividends)
	if err != nil {
		return nil, fmt.Errorf("most recent dividend change: %s", err)
	}

	divsAnnual, err := f.dividendsAnnual(dividends)
	if err != nil {
		return nil, fmt.Errorf("dividends annual: %s", err)
	}

	for _, a := range divsAnnual {
		if payout.PerYear(ticker) != a.PayoutPerYear {
		}
		fmt.Println(ticker, a.Year, a.PayoutPerYear, a.Amount, a.ChangeRate)
	}

	row := &StatsRow{
		Ticker:           ticker,
		DividendChangeMR: mr,
		DividendsAnnual:  divsAnnual,
	}

	return row, nil
}

func (f *StatsGenerator) dividendChangeMostRecent(dividends []*Dividend) (*DividendChangeMR, error) {
	mr := &DividendChangeMR{
		Amount: float64(0),
		Date:   time.Time{},
	}

	if len(dividends) < 2 {
		return mr, nil
	}

	for i := 0; i <= len(dividends)-2; i++ {
		d1 := dividends[i]
		d0 := dividends[i+1]
		if (d1.Amount - d0.Amount) != 0 {
			mr = &DividendChangeMR{
				Amount: ((d1.Amount - d0.Amount) / d0.Amount) * 100,
				Date:   d1.Date,
			}
			break
		}
	}

	return mr, nil
}

func (f *StatsGenerator) dividendsAnnual(dividends []*Dividend) ([]*DividendAnnual, error) {
	divsAnnualMap := map[int]*DividendAnnual{}

	startYear := f.opts.now.Year() - 12
	endYear := f.opts.now.Year() - 1

	for i := startYear; i <= endYear; i++ {
		divsAnnualMap[i] = &DividendAnnual{Year: i}
	}

	for _, d := range dividends {
		if d.Date.Year() < startYear {
			break
		}

		if d.Date.Year() > endYear {
			continue
		}

		a := divsAnnualMap[d.Date.Year()]
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

func parseDividends(p string) ([]*Dividend, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %s", err)
	}

	records := make([]*Dividend, 0)

	for _, row := range rows[1:] {
		div := float64(0)

		date, err := time.Parse("2006-01-02", row[0])
		if err != nil {
			return nil, err
		}

		if row[1] != "null" {
			div, err = strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, err
			}
		}

		record := &Dividend{Date: date, Amount: div}
		records = append(records, record)
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Date.After(records[j].Date)
	})

	return records, nil
}

func (f *StatsGenerator) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}
