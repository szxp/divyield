package chart

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"text/template"
	"time"

	"szakszon.com/divyield"
	"szakszon.com/divyield/logger"
)

type options struct {
	outputDir string
	startDate time.Time
	logger    logger.Logger
	db        divyield.DB
}

type Option func(o options) options

func OutputDir(dir string) Option {
	return func(o options) options {
		o.outputDir = dir
		return o
	}
}

func StartDate(d time.Time) Option {
	return func(o options) options {
		o.startDate = d
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

var defaultOptions = options{
	outputDir: "",
	startDate: time.Time{},
	logger:    nil,
}

func NewChartGenerator(os ...Option) ChartGenerator {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}
	return ChartGenerator{
		opts: opts,
	}
}

type ChartGenerator struct {
	opts options
}

func (f *ChartGenerator) Generate(ctx context.Context, tickers []string) error {
	if f.opts.startDate.IsZero() {
		f.opts.startDate = time.Date(time.Now().UTC().Year()-5, time.January, 1, 1, 0, 0, 0, time.UTC)
	}

	for _, ticker := range tickers {
		yieldsDB, err := f.opts.db.DividendYields(
			ctx, ticker,
			&divyield.DividendYieldFilter{From: f.opts.startDate})

		yields := make([]*dividendYield, 0, len(yieldsDB))

		for i := 0; i < len(yieldsDB); i++ {
			ydb := yieldsDB[i]

			dgr := float64(0)
			if i <= len(yieldsDB)-2 && 0 < yieldsDB[i+1].DividendAdj {
				ydbPrev := yieldsDB[i+1]
				dgr = ((ydb.DividendAdj - ydbPrev.DividendAdj) / ydbPrev.DividendAdj) * 100.0
			}

			y := &dividendYield{
				DividendYield:      ydb,
				DividendGrowthRate: dgr,
			}

			yields = append(yields, y)
		}

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

		err = writeYields(d, yields)
		if err != nil {
			return fmt.Errorf("write data file: %s: %s", dataPath, err)
		}

		minPrice, maxPrice := rangePrices(yields)
		minYield, maxYield := rangeYields(yields)
		minDiv, maxDiv := rangeDividends(yields)
		minDGR, maxDGR := rangeGrowthRates(yields)

		plotParams := plotParams{
			Datafile:       path.Join(f.opts.outputDir, ticker+".csv"),
			Imgfile:        path.Join(f.opts.outputDir, ticker+".png"),
			TitlePrices:    ticker + " prices",
			TitleDivYield:  ticker + " forward dividend yields",
			TitleDividends: ticker + " dividends",
			TitleDGR:       ticker + " dividend growth rates",

			PriceYrMin: math.Max(minPrice-((maxPrice-minPrice)*0.1), 0),
			PriceYrMax: math.Max(maxPrice + ((maxPrice - minPrice) * 0.1), 0.01),

			YieldYrMin: math.Max(minYield-((maxYield-minYield)*0.1), 0),
			YieldYrMax: math.Max(maxYield + ((maxYield - minYield) * 0.1), 0.01),

			DivYrMin: math.Max(minDiv-((maxDiv-minDiv)*0.1), 0),
			DivYrMax: math.Max(maxDiv + ((maxDiv - minDiv) * 0.1), 0.01),

			DGRYrMin: minDGR - ((maxDGR - minDGR) * 0.1),
			DGRYrMax: math.Max(maxDGR + ((maxDGR - minDGR) * 0.1), 0.01),
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

		//fmt.Println("gnuplot -e ", "\""+plotCommandsStr+"\"")
		err = exec.CommandContext(ctx, "gnuplot", "-e", plotCommandsStr).Run()
		if err != nil {
			return err
		}

		f.log("%s: %s", ticker, "OK")
	}
	return nil
}

func (f *ChartGenerator) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}

type dividendYield struct {
	*divyield.DividendYield
	DividendGrowthRate float64
}

func writeYields(out io.Writer, yields []*dividendYield) error {
	w := &writer{W: bufio.NewWriter(out)}

	w.WriteString(
		"Date," +
			"CloseAdj," +
			"DivYield," +
			"DivdAdj," +
			"DGR",
	)

	for i := 0; i < len(yields); i++ {
		y := yields[i]
		w.WriteString("\n")

		row := fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f",
			y.Date.Format("2006-01-02"),
			y.CloseAdj,
			y.ForwardTTM(),
			y.DividendAdj,
			y.DividendGrowthRate,
		)
		w.WriteString(row)
	}

	err := w.Flush()
	if err != nil {
		return err
	}
	return err
}

func rangePrices(yields []*dividendYield) (float64, float64) {
    if len(yields) == 0 {
        return 0,0
    }
	min := yields[0].CloseAdj
	max := yields[0].CloseAdj
	for _, v := range yields {
		if v.CloseAdj < min {
			min = v.CloseAdj
		}
		if v.CloseAdj > max {
			max = v.CloseAdj
		}
	}
	return min, max
}

func rangeYields(yields []*dividendYield) (float64, float64) {
    if len(yields) == 0 {
        return 0,0
    }
	min := yields[0].ForwardTTM()
	max := yields[0].ForwardTTM()
	for _, v := range yields {
		fwd := v.ForwardTTM()
		if fwd < min {
			min = fwd
		}
		if fwd > max {
			max = fwd
		}
	}
	return min, max
}

func rangeDividends(yields []*dividendYield) (float64, float64) {
    if len(yields) == 0 {
        return 0,0
    }
	min := yields[0].DividendAdj
	max := yields[0].DividendAdj
	for _, v := range yields {
		if v.DividendAdj < min {
			min = v.DividendAdj
		}
		if v.DividendAdj > max {
			max = v.DividendAdj
		}
	}
	return min, max
}

func rangeGrowthRates(yields []*dividendYield) (float64, float64) {
    if len(yields) == 0 {
        return 0,0
    }
	min := yields[0].DividendGrowthRate
	max := yields[0].DividendGrowthRate
	for _, v := range yields {
		if v.DividendGrowthRate < min {
			min = v.DividendGrowthRate
		}
		if v.DividendGrowthRate > max {
			max = v.DividendGrowthRate
		}
	}
	return min, max
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

var nlRE = regexp.MustCompile(`\r?\n`)

type plotParams struct {
	Datafile string
	Imgfile  string

	TitlePrices    string
	TitleDivYield  string
	TitleDividends string
	TitleDGR       string

	PriceYrMin float64
	PriceYrMax float64

	YieldYrMin float64
	YieldYrMax float64

	DivYrMin float64
	DivYrMax float64

	DGRYrMin float64
	DGRYrMax float64
}

const plotCommandsTmpl = `
datafile='{{.Datafile}}';
imgfile='{{.Imgfile}}';
titlep='{{.TitlePrices}}';
titledy='{{.TitleDivYield}}';
titled='{{.TitleDividends}}';

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
set size 1, 0.25;

set origin 0.0,0.75;
set title titlep;
set yrange [{{.PriceYrMin}}:{{.PriceYrMax}}];
plot datafile using 1:2 with filledcurves above y = 0;

set origin 0.0,0.50;
set title titledy;
set yrange [{{.YieldYrMin}}:{{.YieldYrMax}}];
plot datafile using 1:3 with filledcurves above y = 0;

set origin 0.0,0.25;
set title titled;
set yrange [{{.DivYrMin}}:{{.DivYrMax}}];
plot datafile using 1:4 with lines lw 4;

set origin 0.0,0.0;
set title '{{.TitleDGR}}';
set yrange [{{.DGRYrMin}}:{{.DGRYrMax}}];
set style fill solid;
set boxwidth 1 absolute;
plot datafile using 1:5 with boxes lw 4;

unset multiplot;
`
