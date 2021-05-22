package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
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
//	case "stats":
//		return c.stats(ctx)
	case "profile":
		return c.profile(ctx)
	case "symbols":
		return c.symbols(ctx)
	case "exchanges":
		return c.exchanges(ctx)
	default:
		return fmt.Errorf("invalid command")
	}
}

func (c *Command) pull(ctx context.Context) error {
	symbols := c.args
	from := c.opts.startDate

	err := c.opts.db.InitSchema(ctx, symbols)
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

		fmt.Println("Price currency", priceCurrency)

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
		fmt.Println("splits:", len(sout.Splits))

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
                fmt.Println("div currency:", v.Currency)
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
		fmt.Println("dividends:", len(dout.Dividends))

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
		fmt.Println("prices:", len(pout.Prices))

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
	c.writef(buf.String())
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
	c.writef(buf.String())
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
	c.writef(buf.String())
}

func (c *Command) writef(format string, v ...interface{}) {
	if c.opts.writer != nil {
		fmt.Fprintf(c.opts.writer, format, v...)
	}
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
