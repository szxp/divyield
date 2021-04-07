package charter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"text/template"
	"time"

	"szakszon.com/divyield/logger"
	"szakszon.com/divyield/payout"
)

type options struct {
	outputDir string
	stocksDir string
	startDate time.Time
	endDate   time.Time
	timeout   time.Duration // http client timeout, 0 means no timeout
	logger    logger.Logger
}

type Option func(o options) options

func OutputDir(dir string) Option {
	return func(o options) options {
		o.outputDir = dir
		return o
	}
}

func StocksDir(dir string) Option {
	return func(o options) options {
		o.stocksDir = dir
		return o
	}
}

func StartDate(d time.Time) Option {
	return func(o options) options {
		o.startDate = d
		return o
	}
}

func EndDate(d time.Time) Option {
	return func(o options) options {
		o.endDate = d
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
	outputDir: "",
	stocksDir: "",
	startDate: time.Date(1900, time.January, 1, 1, 0, 0, 0, time.UTC),
	endDate:   time.Time{},
	logger:    nil,
}

func NewCharter(os ...Option) Charter {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}
	return Charter{
		opts: opts,
	}
}

type Charter struct {
	opts options
	errs []error
}

func (f *Charter) Chart(ctx context.Context, tickers []string) error {
	if f.opts.endDate.IsZero() {
		f.opts.endDate = time.Now()
	}

	for _, ticker := range tickers {
		pricesPath := filepath.Join(f.opts.stocksDir, ticker, "prices.csv")
		prices, err := parsePrices(ticker, pricesPath)
		if err != nil {
			return fmt.Errorf("parse prices: %s: %s", pricesPath, err)
		}

		dividendsPath := filepath.Join(f.opts.stocksDir, ticker, "dividends.csv")
		dividends, err := parseDividends(dividendsPath)
		if err != nil {
			return fmt.Errorf("parse dividends: %s: %s", dividendsPath, err)
		}

		setDividendRecent(prices, dividends)

		err = os.MkdirAll(f.opts.outputDir, 0666)
		if err != nil {
			return fmt.Errorf("create dir: %s", err)
		}

		dataPath := filepath.Join(f.opts.outputDir, ticker+".csv")
		d, err := os.Create(dataPath)
		if err != nil {
			return fmt.Errorf("create data file: %s: %s", dataPath, err)
		}
		defer d.Close()

		err = writePrices(d, prices, f.opts.startDate, f.opts.endDate)
		if err != nil {
			return fmt.Errorf("create data file: %s: %s", dataPath, err)
		}

		plotParams := plotParams{
			Datafile:       path.Join(f.opts.outputDir, ticker+".csv"),
			Imgfile:        path.Join(f.opts.outputDir, ticker+".png"),
			TitlePrices:    ticker + " prices",
			TitleDivYield:  ticker + " forward dividend yield",
			TitleDividends: ticker + " dividends",
		}
		plotCommandsTmpl, err := template.New("plot").Parse(plotCommandsTmpl)
		if err != nil {
			return err
		}

		plotCommands := bytes.NewBufferString("")
		err = plotCommandsTmpl.Execute(plotCommands, plotParams)
		if err != nil {
			return err
		}

		plotCommandsStr := nlRE.ReplaceAllString(plotCommands.String(), " ")
		err = exec.CommandContext(ctx, "gnuplot", "-e", plotCommandsStr).Run()
		if err != nil {
			return err
		}

		f.log("%s: %s", ticker, "OK")
	}
	return nil
}

func (f *Charter) Errs() []error {
	return f.errs
}

func (f *Charter) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}

func writePrices(out io.Writer, prices []*Price, startDate, endDate time.Time) error {
	w := &writer{W: bufio.NewWriter(out)}

	w.WriteString("Date,Price,DividendRecent,DividendForward12M,DividendYieldForward12M,Close")
	for _, p := range prices {

		if !startDate.IsZero() && p.Date.Unix() < startDate.Unix() {
			continue
		}
		if !endDate.IsZero() && endDate.Unix() <= p.Date.Unix() {
			continue
		}

		w.WriteString("\n")
		w.WriteString(p.String())
	}

	err := w.Flush()
	if err != nil {
		return err
	}
	return err
}

func setDividendRecent(prices []*Price, dividends []*Dividend) {
	for _, p := range prices {
	DIVLOOP:
		for _, d := range dividends {
			if !p.Date.Before(d.Date) {
				p.DividendRecent = d.Dividend
				break DIVLOOP
			}
		}
	}
}

type writer struct {
	W   *bufio.Writer
	Err error
}

func (w *writer) Flush() error {
	if w.Err != nil {
		return w.Err
	}
	return w.W.Flush()
}

func (w *writer) WriteString(s string) error {
	if w.Err != nil {
		return w.Err
	}

	_, err := w.W.Write([]byte(s))
	if err != nil {
		w.Err = err
		return err
	}
	return err
}

type Price struct {
	Date           time.Time
	Price          float64
	Close          float64
	DividendRecent float64
	PayoutPerYear  int
}

func (r *Price) String() string {
	divForward12M := r.DividendRecent * float64(r.PayoutPerYear)
	divYield := float64(0)
	if r.Price > 0 {
		divYield = (divForward12M / r.Price) * float64(100)
	}

	return fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%.2f",
		r.Date.Format("2006-01-02"),
		r.Price,
		r.DividendRecent,
		divForward12M,
		divYield,
		r.Close,
	)
}

func parsePrices(ticker, p string) ([]*Price, error) {
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

	records := make([]*Price, 0)

	for _, row := range rows[1:] {
		price := float64(0)
		close := float64(0)

		date, err := time.Parse("2006-01-02", row[0])
		if err != nil {
			return nil, err
		}

		if row[5] != "null" {
			price, err = strconv.ParseFloat(row[5], 64)
			if err != nil {
				return nil, err
			}
			close, err = strconv.ParseFloat(row[4], 64)
			if err != nil {
				return nil, err
			}
		}

		record := &Price{Date: date, Price: price, Close: close, PayoutPerYear: payout.PerYear(ticker)}
		records = append(records, record)
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Date.After(records[j].Date)
	})

	return records, nil
}

type Dividend struct {
	Date     time.Time
	Dividend float64
}

func (r *Dividend) String() string {
	return fmt.Sprintf("%s,%s",
		r.Date.Format("2006-01-02"),
		strconv.FormatFloat(r.Dividend, 'f', -1, 64),
	)
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

		record := &Dividend{Date: date, Dividend: div}
		records = append(records, record)
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Date.After(records[j].Date)
	})

	return records, nil
}

var nlRE = regexp.MustCompile(`\r?\n`)

type plotParams struct {
	Datafile       string
	Imgfile        string
	TitlePrices    string
	TitleDivYield  string
	TitleDividends string
}

const plotCommandsTmpl = `
datafile='{{.Datafile}}';
imgfile='{{.Imgfile}}';
titleprices='{{.TitlePrices}}';
titledivyield='{{.TitleDivYield}}';
titledividends='{{.TitleDividends}}';

set terminal png size 1920,1080;
set output imgfile;

set lmargin  9;
set rmargin  2;

set grid;
set autoscale;
set key outside;
set key bottom right;
set key autotitle columnhead;

set datafile separator ',';
set xdata time;
set timefmt '%Y-%m-%d';
set format x '%Y %b %d';

set multiplot;
set size 1, 0.33;

set origin 0.0,0.66;
set title titleprices;
plot datafile using 1:2 with filledcurves above y = 0;

set origin 0.0,0.33;
plot datafile using 1:6 with filledcurves above y = 0;
#set title titledivyield;
#plot datafile using 1:5 with filledcurves above y = 0;

#set origin 0.0,0.0;
#set title titledividends;
#plot datafile using 1:3 with lines;

unset multiplot;
`
