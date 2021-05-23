package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"szakszon.com/divyield"
)

type Command struct {
	name string
	opts options
	args []string
}

func NewCommand(
	name string,
	args []string,
	os ...Option,
) *Command {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	return &Command{
		name: name,
		opts: opts,
		args: args,
	}
}

func (c *Command) Execute(ctx context.Context) error {
	switch c.name {
	case "pull":
		return c.pull(ctx)
	case "stats":
		return c.stats(ctx)
	case "profile":
		return c.profile(ctx)
	case "symbols":
		return c.symbols(ctx)
	case "exchanges":
		return c.exchanges(ctx)
	default:
		return fmt.Errorf("invalid command: %v", c.name)
	}
}

func (c *Command) stats(ctx context.Context) error {
	var err error

	symbols := c.args
	if len(symbols) == 0 {
		symbols, err = c.symbolsDB(ctx)
		if err != nil {
			return err
		}
	}

	sg := &statsGenerator{
		db:               c.opts.db,
		startDate:        c.opts.startDate,
		divYieldFwdMin:   c.opts.divYieldFwdMin,
		divYieldFwdMax:   c.opts.divYieldFwdMax,
		divYieldTotalMin: c.opts.divYieldTotalMin,
		ggrROI:           c.opts.ggrROI,
		ggrMin:           c.opts.ggrMin,
		ggrMax:           c.opts.ggrMax,
		noCutDividend:    c.opts.noCutDividend,
		noDecliningDGR:   c.opts.noDecliningDGR,
	}

	stats, err := sg.Generate(ctx, symbols)
	if err != nil {
		return err
	}

    if c.opts.chart {
        
    }

	c.writeStats(stats)
	return nil
}

func (c *Command) writeStats(s *divyield.Stats) {
	out := &bytes.Buffer{}
	w := tabwriter.NewWriter(
		out, 0, 0, 2, ' ', tabwriter.AlignRight)

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
		b.WriteString(fmt.Sprintf(
			"%s",
			row.DividendChangeMRDate.Format("2006-01-02")))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf(
			"%.2f%%",
			row.DividendChangeMR))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[1]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[2]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[3]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[4]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[5]))
		b.WriteByte('\t')

		fmt.Fprintln(w, b.String())
	}

	//fmt.Fprintln(w, "")

	w.Flush()

	c.writef("%s", out.String())
}

func (c *Command) pull(ctx context.Context) error {
	var err error
	from := c.opts.startDate

	symbols := c.args
	if len(symbols) == 0 {
		symbols, err = c.symbolsDB(ctx)
		if err != nil {
			return err
		}
	}

	err = c.opts.db.InitSchema(ctx, symbols)
	if err != nil {
		return fmt.Errorf("init schema: %v", err)
	}

	eout, err := c.opts.exchangeService.Fetch(
		ctx,
		&divyield.ExchangeFetchInput{},
	)
	if err != nil {
		return err
	}

	for _, symbol := range symbols {
		proout, err := c.opts.profileService.Fetch(
			ctx,
			&divyield.ProfileFetchInput{
				Symbol: symbol,
			},
		)
		if err != nil {
			return err
		}

		if proout.Profile == nil {
			return fmt.Errorf("profile not found: " + symbol)
		}

		var priceCurrency string
		dashIdx := strings.LastIndexByte(symbol, '-')
		if dashIdx != -1 {
			symbolSuffix := symbol[dashIdx:]
			for _, ex := range eout.Exchanges {
				if ex.Suffix == symbolSuffix {
					priceCurrency = ex.Currency
				}
			}
			if priceCurrency == "" {
				return fmt.Errorf(
					"currency not found: %v",
					symbolSuffix,
				)
			}
		} else {
			priceCurrency = "USD"
		}

		fromSplits := from
		if !c.opts.reset {
			fromSplits, err = c.adjustFromSplits(
				ctx,
				symbol,
				from,
			)
			if err != nil {
				return err
			}
		}

		sout, err := c.opts.splitService.Fetch(
			ctx,
			&divyield.SplitFetchInput{
				Symbol: symbol,
				From:   fromSplits,
			},
		)
		if err != nil {
			return err
		}
		c.writef("%v: %v splits", symbol, len(sout.Splits))

		fromDividends := from
		if !c.opts.reset {
			fromDividends, err = c.adjustFromDividends(
				ctx,
				symbol,
				from,
			)
			if err != nil {
				return err
			}
		}

		dout, err := c.opts.dividendService.Fetch(
			ctx,
			&divyield.DividendFetchInput{
				Symbol: symbol,
				From:   fromDividends,
			},
		)
		if err != nil {
			return err
		}
		for _, v := range dout.Dividends {
			if v.Currency != priceCurrency {
				ccout, err := c.opts.currencyService.Convert(
					ctx,
					&divyield.CurrencyConvertInput{
						From:   v.Currency,
						To:     priceCurrency,
						Amount: v.Amount,
						Date:   v.ExDate,
					},
				)
				if err != nil {
					return err
				}

				v.Currency = priceCurrency
				v.Amount = ccout.Amount
			}
		}
		c.writef("%v: %v dividends", symbol, len(dout.Dividends))

		fromPrices := from
		if !c.opts.reset {
			fromPrices, err = c.adjustFromPrices(
				ctx,
				symbol,
				from,
			)
			if err != nil {
				return err
			}
		}

		pout, err := c.opts.priceService.Fetch(
			ctx,
			&divyield.PriceFetchInput{
				Symbol: symbol,
				From:   fromPrices,
			},
		)
		if err != nil {
			return err
		}
		for _, v := range pout.Prices {
			v.Currency = priceCurrency
		}
		c.writef("%v: %v prices", symbol, len(pout.Prices))

		_, err = c.opts.db.SaveProfile(
			ctx,
			&divyield.DBSaveProfileInput{
				Symbol:  symbol,
				Profile: proout.Profile,
			},
		)
		if err != nil {
			return err
		}

		_, err = c.opts.db.SaveSplits(
			ctx,
			&divyield.DBSaveSplitsInput{
				Symbol: symbol,
				Splits: sout.Splits,
				Reset:  c.opts.reset,
			},
		)
		if err != nil {
			return err
		}

		_, err = c.opts.db.SaveDividends(
			ctx,
			&divyield.DBSaveDividendsInput{
				Symbol:    symbol,
				Dividends: dout.Dividends,
				Reset:     c.opts.reset,
			},
		)
		if err != nil {
			return err
		}

		_, err = c.opts.db.SavePrices(
			ctx,
			&divyield.DBSavePricesInput{
				Symbol: symbol,
				Prices: pout.Prices,
				Reset:  c.opts.reset,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Command) symbolsDB(
	ctx context.Context,
) ([]string, error) {
	symbols := make([]string, 0)
	out, err := c.opts.db.Profiles(
		ctx,
		&divyield.DBProfilesInput{},
	)
	if err != nil {
		return nil, err
	}
	for _, v := range out.Profiles {
		symbols = append(symbols, v.Symbol)
	}
	return symbols, nil
}

func (c *Command) adjustFromSplits(
	ctx context.Context,
	symbol string,
	from time.Time,
) (time.Time, error) {
	latest, err := c.opts.db.Splits(
		ctx, symbol, &divyield.SplitFilter{Limit: 1})
	if err != nil {
		return time.Time{}, err
	}
	if len(latest) == 0 {
		return from, nil
	}
	return latest[0].ExDate.AddDate(0, 0, 1), nil
}

func (c *Command) adjustFromDividends(
	ctx context.Context,
	symbol string,
	from time.Time,
) (time.Time, error) {
	latest, err := c.opts.db.Dividends(
		ctx, symbol, &divyield.DividendFilter{Limit: 1})
	if err != nil {
		return time.Time{}, err
	}
	if len(latest) == 0 {
		return from, nil
	}
	return latest[0].ExDate.AddDate(0, 0, 1), nil
}

func (c *Command) adjustFromPrices(
	ctx context.Context,
	symbol string,
	from time.Time,
) (time.Time, error) {
	latest, err := c.opts.db.Prices(
		ctx, symbol, &divyield.PriceFilter{Limit: 1})
	if err != nil {
		return time.Time{}, err
	}
	if len(latest) == 0 {
		return from, nil
	}
	return latest[0].Date.AddDate(0, 0, 1), nil
}

func (c *Command) profile(ctx context.Context) error {
	in := &divyield.ProfileFetchInput{
		Symbol: c.args[0],
	}

	out, err := c.opts.profileService.Fetch(ctx, in)
	if err != nil {
		return err
	}

	if out.Profile == nil {
		c.writef("Not found: %v", in.Symbol)
		return nil
	}
	c.writeProfile(out.Profile)
	return nil
}

func (c *Command) writeProfile(cp *divyield.Profile) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}
	b.WriteString("Symbol:")
	b.WriteByte('\t')
	b.WriteString(cp.Symbol)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Name:")
	b.WriteByte('\t')
	b.WriteString(cp.Name)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Exchange:")
	b.WriteByte('\t')
	b.WriteString(cp.Exchange)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Issue type:")
	b.WriteByte('\t')
	b.WriteString(cp.IssueType)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Industry:")
	b.WriteByte('\t')
	b.WriteString(cp.Industry)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Sector:")
	b.WriteByte('\t')
	b.WriteString(cp.Sector)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Description:")
	b.WriteByte('\t')
	b.WriteString(cp.Description)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Website:")
	b.WriteByte('\t')
	b.WriteString(cp.Website)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Address:")
	b.WriteByte('\t')
	b.WriteString(cp.Address)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("State:")
	b.WriteByte('\t')
	b.WriteString(cp.State)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("City:")
	b.WriteByte('\t')
	b.WriteString(cp.City)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Zip:")
	b.WriteByte('\t')
	b.WriteString(cp.Zip)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Country:")
	b.WriteByte('\t')
	b.WriteString(cp.Country)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Phone:")
	b.WriteByte('\t')
	b.WriteString(cp.Phone)
	fmt.Fprintln(w, b.String())

	w.Flush()
	c.writef("%s", buf.String())
}

func (c *Command) symbols(ctx context.Context) error {
	in := &divyield.ISINResolveInput{
		ISIN: c.args[0],
	}

	out, err := c.opts.isinService.Resolve(ctx, in)
	if err != nil {
		return err
	}

	c.writeSymbolISINs(out.Symbols)
	return nil
}

func (c *Command) writeSymbolISINs(symbols []*divyield.SymbolISIN) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}
	b.WriteString("Region")
	b.WriteByte('\t')
	b.WriteString("Exchange")
	b.WriteByte('\t')
	b.WriteString("Symbol")
	fmt.Fprintln(w, b.String())

	for _, v := range symbols {
		b.Reset()
		b.WriteString(v.Region)
		b.WriteByte('\t')
		b.WriteString(v.Exchange)
		b.WriteByte('\t')
		b.WriteString(v.Symbol)
		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", buf.String())
}

func (c *Command) exchanges(ctx context.Context) error {
	in := &divyield.ExchangeFetchInput{}
	out, err := c.opts.exchangeService.Fetch(ctx, in)
	if err != nil {
		return err
	}

	c.writeExchanges(out.Exchanges)
	return nil
}

func (c *Command) writeExchanges(exchanges []*divyield.Exchange) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}
	b.WriteString("Region")
	b.WriteByte('\t')
	b.WriteString("Exchange")
	b.WriteByte('\t')
	b.WriteString("Suffix")
	b.WriteByte('\t')
	b.WriteString("Currency")
	b.WriteByte('\t')
	b.WriteString("Description")
	fmt.Fprintln(w, b.String())

	for _, v := range exchanges {
		b.Reset()
		b.WriteString(v.Region)
		b.WriteByte('\t')
		b.WriteString(v.Exchange)
		b.WriteByte('\t')
		b.WriteString(v.Suffix)
		b.WriteByte('\t')
		b.WriteString(v.Currency)
		b.WriteByte('\t')
		b.WriteString(v.Description)
		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", buf.String())
}

func (c *Command) writef(format string, v ...interface{}) {
	if c.opts.writer != nil {
		fmt.Fprintf(c.opts.writer, format, v...)
	}
}

type statsGenerator struct {
	db              divyield.DB
	writer          io.Writer
	startDate       time.Time
	splitService    divyield.SplitService
	dividendService divyield.DividendService
	priceService    divyield.PriceService

	divYieldFwdMin   float64
	divYieldFwdMax   float64
	divYieldTotalMin float64
	ggrROI           float64
	ggrMin           float64
	ggrMax           float64
	noCutDividend    bool
	noDecliningDGR   bool
}

func (g *statsGenerator) Generate(
	ctx context.Context,
	symbols []string,
) (*divyield.Stats, error) {
	var workerWg sync.WaitGroup
	var resultWg sync.WaitGroup
	resultCh := make(chan result)

	stats := &divyield.Stats{}

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
	for _, ticker := range symbols {
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
			row, err := g.generateStatsRow(ctx, ticker)
			resultCh <- result{Ticker: ticker, Row: row, Err: err}
		}(ticker)
	}

	workerWg.Wait()
	close(resultCh)
	resultWg.Wait()

	if len(errs) > 0 {
		return nil, errs[0]
	}

	g.filter(
		stats,
		g.filterDivYieldFwdMinMax,
		g.filterDivYieldTotalMin,
		g.filterGGRMinMax,
		g.filterNoCutDividend,
		g.filterNoDecliningDGR,
	)

	return stats, nil
}

type result struct {
	Ticker string
	Row    *divyield.StatsRow
	Err    error
}

func (g *statsGenerator) generateStatsRow(
	ctx context.Context,
	ticker string,
) (*divyield.StatsRow, error) {

	dyf := &divyield.DividendYieldFilter{
		Limit: 1,
	}
	dividendYields, err := g.db.DividendYields(ctx, ticker, dyf)
	if err != nil {
		return nil, fmt.Errorf("get dividend yields: %s", err)
	}

	divYieldFwd := float64(0)
	divFwd := float64(0)
	ggr := float64(0)
	if len(dividendYields) > 0 {
		divYieldFwd = dividendYields[0].ForwardTTM()
		divFwd = dividendYields[0].DividendForwardTTM()
	}
	if g.ggrROI > 0 {
		ggr = g.ggrROI - divYieldFwd
	}

	df := &divyield.DividendFilter{
		From: time.Date(
			time.Now().UTC().Year()-11, time.January, 1,
			0, 0, 0, 0, time.UTC),
		CashOnly: true,
		Regular:  true,
	}
	dividendsDB, err := g.db.Dividends(ctx, ticker, df)
	if err != nil {
		return nil, fmt.Errorf("get dividends: %s", err)
	}

	dividends := make([]*divyield.DividendChange, 0, len(dividendsDB))
	for _, d := range dividendsDB {
		dividends = append(dividends, &divyield.DividendChange{
			Dividend: d,
		})
	}
	g.calcDividendChanges(dividends)

	divChangeMR, divChangeMRDate := g.dividendChangeMR(dividends)

	row := &divyield.StatsRow{
		Ticker:               ticker,
		DivYieldFwd:          divYieldFwd,
		DivFwd:               divFwd,
		GordonGrowthRate:     ggr,
		Dividends:            dividends,
		DividendChangeMR:     divChangeMR,
		DividendChangeMRDate: divChangeMRDate,
		DGRs: map[int]float64{
			1: g.dgr(dividends, 1),
			2: g.dgr(dividends, 2),
			3: g.dgr(dividends, 3),
			4: g.dgr(dividends, 4),
			5: g.dgr(dividends, 5),
		},
	}

	return row, nil
}

func (g *statsGenerator) calcDividendChanges(
	dividends []*divyield.DividendChange,
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

func (g *statsGenerator) filter(
	stats *divyield.Stats,
	filters ...filterFunc,
) {
	filtered := make([]*divyield.StatsRow, 0, len(stats.Rows))

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

type filterFunc func(row *divyield.StatsRow) bool

func (g *statsGenerator) filterNoCutDividend(
	row *divyield.StatsRow,
) bool {
	if !g.noCutDividend {
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

func (g *statsGenerator) filterDivYieldFwdMinMax(
	row *divyield.StatsRow,
) bool {
	min := g.divYieldFwdMin
	max := g.divYieldFwdMax

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

func (g *statsGenerator) filterDivYieldTotalMin(
	row *divyield.StatsRow,
) bool {
	min := g.divYieldTotalMin
	if min <= 0 {
		return true
	}

	return min <= row.DivYieldFwd+row.DGRs[5]
}

func (g *statsGenerator) filterGGRMinMax(
	row *divyield.StatsRow,
) bool {
	min := g.ggrMin
	max := g.ggrMax

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

func (g *statsGenerator) filterNoDecliningDGR(
	row *divyield.StatsRow,
) bool {
	if !g.noDecliningDGR {
		return true
	}

	dgrs := []float64{
		row.DGRs[5],
		row.DGRs[4],
		row.DGRs[3],
		row.DGRs[2],
		row.DGRs[1],
		row.DividendChangeMR,
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

func (g *statsGenerator) writef(
	format string,
	v ...interface{},
) {
	if g.writer != nil {
		fmt.Fprintf(g.writer, format, v...)
	}
}

func (g *statsGenerator) dgr(
	dividends []*divyield.DividendChange,
	n int,
) float64 {
	if n < 1 {
		panic("n must be greater than 1")
	}

	if len(dividends) == 0 {
		return 0
	}

	y := time.Now().UTC().Year()
	ed := time.Date(y-1, time.December, 31, 0, 0, 0, 0, time.UTC)
	sd := time.Date(y-n, time.January, 1, 0, 0, 0, 0, time.UTC)

	changes := make([]float64, 0, n)

	//sum := float64(0)
	//c := 0
	for _, v := range dividends {
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

		if len(changes)%2 == 1 {
			dgr = changes[(len(changes) / 2)]
		} else {
			vl := changes[len(changes)/2-1]
			vr := changes[len(changes)/2]
			dgr = (vl + vr) / 2.0
		}
	}

	return dgr
}

func (g *statsGenerator) dividendChangeMR(
	dividends []*divyield.DividendChange,
) (float64, time.Time) {
	for _, v := range dividends {
		if 0 < v.Change {
			return v.Change, v.ExDate
		}
	}
	return float64(0), time.Time{}
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

var defaultOptions = options{
	writer: nil,
}

type options struct {
	db              divyield.DB
	writer          io.Writer
	dir             string
	dryRun          bool
	startDate       time.Time
	reset           bool
	profileService  divyield.ProfileService
	isinService     divyield.ISINService
	exchangeService divyield.ExchangeService
	splitService    divyield.SplitService
	dividendService divyield.DividendService
	priceService    divyield.PriceService
	currencyService divyield.CurrencyService

	divYieldFwdMin   float64
	divYieldFwdMax   float64
	divYieldTotalMin float64
	ggrROI           float64
	ggrMin           float64
	ggrMax           float64
	noCutDividend    bool
	noDecliningDGR   bool
	chart   bool
}

type Option func(o options) options

func Writer(v io.Writer) Option {
	return func(o options) options {
		o.writer = v
		return o
	}
}

func Dir(v string) Option {
	return func(o options) options {
		o.dir = v
		return o
	}
}

func DryRun(v bool) Option {
	return func(o options) options {
		o.dryRun = v
		return o
	}
}

func StartDate(v time.Time) Option {
	return func(o options) options {
		o.startDate = v
		return o
	}
}

func Reset(v bool) Option {
	return func(o options) options {
		o.reset = v
		return o
	}
}

func ProfileService(v divyield.ProfileService) Option {
	return func(o options) options {
		o.profileService = v
		return o
	}
}

func ISINService(v divyield.ISINService) Option {
	return func(o options) options {
		o.isinService = v
		return o
	}
}

func ExchangeService(v divyield.ExchangeService) Option {
	return func(o options) options {
		o.exchangeService = v
		return o
	}
}

func SplitService(v divyield.SplitService) Option {
	return func(o options) options {
		o.splitService = v
		return o
	}
}

func DividendService(v divyield.DividendService) Option {
	return func(o options) options {
		o.dividendService = v
		return o
	}
}

func PriceService(v divyield.PriceService) Option {
	return func(o options) options {
		o.priceService = v
		return o
	}
}

func CurrencyService(v divyield.CurrencyService) Option {
	return func(o options) options {
		o.currencyService = v
		return o
	}
}
func DB(db divyield.DB) Option {
	return func(o options) options {
		o.db = db
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

func Chart(v bool) Option {
	return func(o options) options {
		o.chart = v
		return o
	}
}
