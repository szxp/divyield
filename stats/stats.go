package stats

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	stocksDir           string
	now                 time.Time
	logger              logger.Logger
	db                  divyield.DB
	dividendYieldMin    float64
	dividendYieldMax    float64
	expectedROI         float64
	gordonGrowthRateMin float64
	gordonGrowthRateMax float64
	noCutDividend       bool
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

func ExpectedROI(v float64) Option {
	return func(o options) options {
		o.expectedROI = v
		return o
	}
}

func GordonGrowthRateMin(v float64) Option {
	return func(o options) options {
		o.gordonGrowthRateMin = v
		return o
	}
}

func GordonGrowthRateMax(v float64) Option {
	return func(o options) options {
		o.gordonGrowthRateMax = v
		return o
	}
}

func NoCutDividend(v bool) Option {
	return func(o options) options {
		o.noCutDividend = v
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
	opts  options
	stats *Stats
}

type StatsRow struct {
	Ticker               string
	ForwardDividendYield float64
	GordonGrowthRate     float64
	ForwardDividend      float64
	DividendChangeMR     *DividendChangeMR
	DGR1y                float64
	DGR3y                float64
	DGR5y                float64
	DGR10y               float64
	DividendChanges      []*DividendChange
}

func (r *StatsRow) DGR(n int) float64 {
	if len(r.DividendChanges) == 0 {
		return 0
	}

	return 0

	//     endYear := r.DividendChanges[0].Year()
	//     startYear := endYear - n
	// 	sum := float64(0)
	//     c := 0
	// 	for _, a := range r.DividendChanges {
	// 		if startYear < a.Year() && a.Year() <= endYear {
	//             sum += a.ChangeRate
	//             c += 1
	//         }
	// 	}
	// 	return sum / float64(c)
}

type DividendChangeMR struct {
	ChangePercent float64
	Date          time.Time
}

type DividendChange struct {
	*divyield.Dividend
	ChangeRate float64 // compared to the year before
}

type Stats struct {
	Rows                []*StatsRow
	DividendYieldMin    float64
	DividendYieldMax    float64
	GordonGrowthRateMin float64
	GordonGrowthRateMax float64
	NoCutDividend       bool
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
	b.WriteString("Gordon growth rate")
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
	// 	for _, d := range s.Rows[0].DividendChanges {
	// 		b.WriteString("DGR-" + strconv.Itoa(d.Year) + " (DPS)")
	// 		b.WriteByte('\t')
	// 	}
	// }
	fmt.Fprintln(w, b.String())

	for _, row := range s.Rows {
		b.Reset()
		b.WriteString(fmt.Sprintf("%-6v", row.Ticker))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.ForwardDividendYield))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f", row.ForwardDividend))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.GordonGrowthRate))
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

		// for _, d := range row.DividendChanges {
		// 	b.WriteString(fmt.Sprintf("%.2f%% (%.2f)", d.ChangeRate, d.Amount))
		// 	b.WriteByte('\t')
		// }

		fmt.Fprintln(w, b.String())
	}

	fmt.Fprintln(w, "")

	s.printFooter(w)

	w.Flush()
	return out.String()
}

func (s *Stats) filter() {
	filtered := make([]*StatsRow, 0, len(s.Rows))
	removable := make(map[int]struct{})

	for i, row := range s.Rows {

		if s.NoCutDividend && !isNoCutDividend(row.DividendChanges) {
			removable[i] = struct{}{}
			continue
		}

		y := row.ForwardDividendYield
		ggr := row.GordonGrowthRate

		if s.DividendYieldMin > 0 &&
			(isNaN(y) || y < s.DividendYieldMin) {
			removable[i] = struct{}{}
			continue
		}
		if s.DividendYieldMax > 0 &&
			(isNaN(y) || s.DividendYieldMax < y) {
			removable[i] = struct{}{}
			continue
		}

		if s.GordonGrowthRateMin > 0 &&
			(isNaN(ggr) || ggr < s.GordonGrowthRateMin) {
			removable[i] = struct{}{}
			continue
		}
		if s.GordonGrowthRateMax > 0 &&
			(isNaN(ggr) || s.GordonGrowthRateMax < ggr) {
			removable[i] = struct{}{}
			continue
		}
	}

	for i, row := range s.Rows {
		if _, ok := removable[i]; !ok {
			filtered = append(filtered, row)
		}
	}
	s.Rows = filtered
}

func isNoCutDividend(dividends []*DividendChange) bool {
	for i := 0; i <= len(dividends)-2; i++ {
		d0 := dividends[i]
		if d0.ChangeRate < 0 {
			return false
		}
	}
	return true
}

func (s *Stats) printFooter(w io.Writer) {
    if s.NoCutDividend {
        fmt.Fprintln(w, "No cut dividend")
    }
	if s.DividendYieldMin > 0 {
		fmt.Fprintln(w, "Min dividend yield:",
			strconv.FormatFloat(s.DividendYieldMin, 'f', 2, 64)+"%")
	}
	if s.DividendYieldMax > 0 {
		fmt.Fprintln(w, "Max dividend yield:",
			strconv.FormatFloat(s.DividendYieldMax, 'f', 2, 64)+"%")
	}

	if s.GordonGrowthRateMin > 0 {
		fmt.Fprintln(w, "Min Gordon growth rate:",
			strconv.FormatFloat(s.GordonGrowthRateMin, 'f', 2, 64)+"%")
	}
	if s.GordonGrowthRateMax > 0 {
		fmt.Fprintln(w, "Max Gordon growth rate:",
			strconv.FormatFloat(s.GordonGrowthRateMax, 'f', 2, 64)+"%")
	}

	return
}

func isNaN(v float64) bool {
	return math.IsNaN(v) || math.IsInf(v, 1) || math.IsInf(v, -1)
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
		DividendYieldMin:    f.opts.dividendYieldMin,
		DividendYieldMax:    f.opts.dividendYieldMax,
		GordonGrowthRateMin: f.opts.gordonGrowthRateMin,
		GordonGrowthRateMax: f.opts.gordonGrowthRateMax,
		NoCutDividend:       f.opts.noCutDividend,
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

	stats.filter()
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
	gordonGrowthRate := float64(0)
	if len(dividendYields) > 0 {
		forwardDivYield = dividendYields[0].ForwardTTM()
		forwardDiv = dividendYields[0].ForwardDividend()
		gordonGrowthRate = f.opts.expectedROI - forwardDivYield
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

	divChanges, err := f.dividendChanges(dividends)
	if err != nil {
		return nil, fmt.Errorf("dividends annual: %s", err)
	}

	row := &StatsRow{
		Ticker:               ticker,
		ForwardDividendYield: forwardDivYield,
		ForwardDividend:      forwardDiv,
		GordonGrowthRate:     gordonGrowthRate,
		DividendChangeMR:     mr,
		DividendChanges:      divChanges,
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

func (f *StatsGenerator) dividendChanges(
	dividends []*divyield.Dividend,
) ([]*DividendChange, error) {

	changes := make([]*DividendChange, 0, len(dividends))

	endYear := f.opts.now.Year() - 1
	startYear := f.opts.now.Year() - 12

	for _, d := range dividends {
		if d.ExDate.Year() < startYear {
			break
		}

		if d.ExDate.Year() > endYear {
			continue
		}

		changes = append(changes, &DividendChange{Dividend: d})
	}

	for i := 0; i <= len(changes)-2; i++ {
		a0 := changes[i]
		a1 := changes[i+1]
		a0.ChangeRate = ((a0.AmountAdj - a1.AmountAdj) / a1.AmountAdj) * 100
	}

	return changes, nil
}

func (f *StatsGenerator) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}
