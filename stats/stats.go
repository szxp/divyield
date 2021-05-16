package stats

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"text/tabwriter"
	"time"

	"szakszon.com/divyield"
	"szakszon.com/divyield/logger"
)

type options struct {
	stocksDir string
	now       time.Time
	logger    logger.Logger
	db        divyield.DB
	startDate time.Time

	// filters
	divYieldFwdMin   float64
	divYieldFwdMax   float64
	divYieldTotalMin float64
	ggrROI           float64
	ggrMin           float64
	ggrMax           float64
	noCutDividend    bool
	noDecliningDGR   bool
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

func StartDate(d time.Time) Option {
	return func(o options) options {
		o.startDate = d
		return o
	}
}

func DividendYieldForwardMin(v float64) Option {
	return func(o options) options {
		o.divYieldFwdMin = v
		return o
	}
}

func DividendYieldTotalMin(v float64) Option {
	return func(o options) options {
		o.divYieldTotalMin = v
		return o
	}
}

func DividendYieldForwardMax(v float64) Option {
	return func(o options) options {
		o.divYieldFwdMax = v
		return o
	}
}

func GordonROI(v float64) Option {
	return func(o options) options {
		o.ggrROI = v
		return o
	}
}

func GordonGrowthRateMin(v float64) Option {
	return func(o options) options {
		o.ggrMin = v
		return o
	}
}

func GordonGrowthRateMax(v float64) Option {
	return func(o options) options {
		o.ggrMax = v
		return o
	}
}

func NoCutDividend(v bool) Option {
	return func(o options) options {
		o.noCutDividend = v
		return o
	}
}

func NoDecliningDGR(v bool) Option {
	return func(o options) options {
		o.noDecliningDGR = v
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

func (f *StatsGenerator) Generate(
	ctx context.Context,
	tickers []string,
) (*Stats, error) {
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

	f.filter(
		stats,
		f.filterDivYieldFwdMinMax,
        f.filterDivYieldTotalMin,
		f.filterGGRMinMax,
		f.filterNoCutDividend,
		f.filterNoDecliningDGR,
	)

	return stats, nil
}

type result struct {
	Ticker string
	Row    *StatsRow
	Err    error
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

	divYieldFwd := float64(0)
	divFwd := float64(0)
	ggr := float64(0)
	if len(dividendYields) > 0 {
		divYieldFwd = dividendYields[0].ForwardTTM()
		divFwd = dividendYields[0].DividendForwardTTM()
		ggr = f.opts.ggrROI - divYieldFwd
	}

	df := &divyield.DividendFilter{
		From: time.Date(
			f.opts.now.UTC().Year()-11, time.January, 1,
			0, 0, 0, 0, time.UTC),
		CashOnly: true,
		Regular:  true,
	}
	dividendsDB, err := f.opts.db.Dividends(ctx, ticker, df)
	if err != nil {
		return nil, fmt.Errorf("get dividends: %s", err)
	}

	dividends := make([]*dividend, 0, len(dividendsDB))
	for _, d := range dividendsDB {
		dividends = append(dividends, &dividend{Dividend: d})
	}
	f.calcDividendChanges(dividends)

	row := &StatsRow{
		Ticker:           ticker,
		DivYieldFwd:      divYieldFwd,
		DivFwd:           divFwd,
		GordonGrowthRate: ggr,
		Dividends:        dividends,
		dgrCache:         make(map[int]float64),
	}

	return row, nil
}

func (f *StatsGenerator) calcDividendChanges(
	dividends []*dividend,
) {
	for i := 0; i <= len(dividends)-2; i++ {
		a0 := dividends[i]
		a0.Change = math.NaN()
		a1 := dividends[i+1]
		a1.Change = math.NaN()

		if a0.Currency == a1.Currency {
			a0.Change = ((a0.AmountAdj / a1.AmountAdj) - 1) * 100
		}
	}
}

func (f *StatsGenerator) filter(
	stats *Stats,
	filters ...filterFunc,
) {
	filtered := make([]*StatsRow, 0, len(stats.Rows))

LOOP_ROWS:
	for _, row := range stats.Rows {
		for _, fn := range filters {
			if ok := fn(row); !ok {
				continue LOOP_ROWS
			}
		}
		filtered = append(filtered, row)
	}
	stats.Rows = filtered
}

type filterFunc func(row *StatsRow) bool

func (f *StatsGenerator) filterNoCutDividend(row *StatsRow) bool {
	if !f.opts.noCutDividend {
		return true
	}

	for i := 0; i <= len(row.Dividends)-2; i++ {
		d0 := row.Dividends[i]
		if d0.Change < 0 {
			return false
		}
	}
	return true
}

func (f *StatsGenerator) filterDivYieldFwdMinMax(row *StatsRow) bool {
	min := f.opts.divYieldFwdMin
	max := f.opts.divYieldFwdMax

	if min <= 0 && max <= 0 {
		return true
	}

	v := row.DivYieldFwd

	if min > 0 && (isNaN(v) || v < min) {
		return false
	}

	if max > 0 && (isNaN(v) || max < v) {
		return false
	}

	return true
}

func (f *StatsGenerator) filterDivYieldTotalMin(row *StatsRow) bool {
	min := f.opts.divYieldTotalMin
	if min <= 0 {
		return true
	}

	return min <= row.DivYieldFwd+row.DGR(5)
}

func (f *StatsGenerator) filterGGRMinMax(row *StatsRow) bool {
	min := f.opts.ggrMin
	max := f.opts.ggrMax

	if min <= 0 && max <= 0 {
		return true
	}

	v := row.GordonGrowthRate

	if min > 0 && (isNaN(v) || v < min) {
		return false
	}

	if max > 0 && (isNaN(v) || max < v) {
		return false
	}

	return true
}

func (f *StatsGenerator) filterNoDecliningDGR(row *StatsRow) bool {
	if !f.opts.noDecliningDGR {
		return true
	}

	dgrs := []float64{
		row.DGR(5),
		row.DGR(4),
		row.DGR(3),
		row.DGR(2),
		row.DGR(1),
		row.DividendChangeMR(),
	}

	dgrsPos := make([]float64, 0, len(dgrs))
	for _, v := range dgrs {
		if v > 0 {
			dgrsPos = append(dgrsPos, v)
		}
	}

	for i := 0; i <= len(dgrsPos)-2; i++ {
		v0 := dgrsPos[i]
		v1 := dgrsPos[i+1]

		if v0 < v1 {
			return true
		}
	}

	return false
}

func (f *StatsGenerator) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
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
	b.WriteString("Forward dividend")
	b.WriteByte('\t')
	b.WriteString("Forward yield")
	b.WriteByte('\t')
	b.WriteString("GGR")
	b.WriteByte('\t')
	b.WriteString("MR% date")
	b.WriteByte('\t')
	b.WriteString("MR%")
	b.WriteByte('\t')
	b.WriteString("DGR-1y")
	b.WriteByte('\t')
	b.WriteString("DGR-2y")
	b.WriteByte('\t')
	b.WriteString("DGR-3y")
	b.WriteByte('\t')
	b.WriteString("DGR-4y")
	b.WriteByte('\t')
	b.WriteString("DGR-5y")
	b.WriteByte('\t')

	fmt.Fprintln(w, b.String())

	for _, row := range s.Rows {
		b.Reset()
		b.WriteString(fmt.Sprintf("%-6v", row.Ticker))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f", row.DivFwd))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DivYieldFwd))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.GordonGrowthRate))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%s",
			row.DividendChangeMRDate().Format("2006-01-02")))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DividendChangeMR()))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(1)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(2)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(3)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(4)))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGR(5)))
		b.WriteByte('\t')

		fmt.Fprintln(w, b.String())
	}

	//fmt.Fprintln(w, "")

	w.Flush()
	return out.String()
}

type StatsRow struct {
	Ticker           string
	DivYieldFwd      float64
	DivFwd           float64
	GordonGrowthRate float64
	Dividends        []*dividend

	dgrCache map[int]float64
}

func (r *StatsRow) DGR(n int) float64 {
	if n < 1 {
		panic("n must be greater than 1")
	}

	if v, ok := r.dgrCache[n]; ok {
		return v
	}

	if len(r.Dividends) == 0 {
		return 0
	}

	y := time.Now().UTC().Year()
	ed := time.Date(y-1, time.December, 31, 0, 0, 0, 0, time.UTC)
	sd := time.Date(y-n, time.January, 1, 0, 0, 0, 0, time.UTC)

    changes := make([]float64, 0, n)

	//sum := float64(0)
	//c := 0
	for _, v := range r.Dividends {
		if v.Change > 0 &&
			sd.Unix() < v.ExDate.Unix() &&
			v.ExDate.Unix() < ed.Unix() {
			//sum += v.Change
			//c += 1
            changes = append(changes, v.Change)
		}
	}

	dgr := float64(0)
	if 0 < len(changes) {
        sort.Float64s(changes)

		//dgr = sum / float64(c)

        if len(changes) % 2 == 1 {
            dgr = changes[(len(changes)/2)]
        } else {
            vl := changes[len(changes)/2-1]
            vr := changes[len(changes)/2]
            dgr = (vl + vr) / 2.0
        }
	}

	r.dgrCache[n] = dgr
	return dgr
}

func (r *StatsRow) DividendChangeMR() float64 {
	for _, v := range r.Dividends {
		if 0 < v.Change {
			return v.Change
		}
	}
	return float64(0)
}

func (r *StatsRow) DividendChangeMRDate() time.Time {
	for _, v := range r.Dividends {
		if 0 < v.Change {
			return v.ExDate
		}
	}
	return time.Time{}
}

type StatsError struct {
	Ticker string
	Err    error
}

func (e *StatsError) Error() string {
	return fmt.Sprintf("%s: %s", e.Ticker, e.Err)
}

type dividend struct {
	*divyield.Dividend
	Change float64 // compared to the year before
}

func isNaN(v float64) bool {
	return math.IsNaN(v) || math.IsInf(v, 1) || math.IsInf(v, -1)
}
