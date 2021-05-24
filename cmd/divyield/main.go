package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"szakszon.com/divyield/cli"
	"szakszon.com/divyield/iexcloud"
	"szakszon.com/divyield/postgres"
	"szakszon.com/divyield/xrates"
)

const defaultStocksDir = "work/stocks"
const defaultChartOutputDir = "work/charts"

func main() {
	var err error
	ctx := context.Background()
	ctx, ctxCancel := context.WithCancel(ctx)

	stdoutSync := &StdoutSync{
		mu: &sync.RWMutex{},
		w:  os.Stdout,
	}

	termCh := make(chan os.Signal)
	signal.Notify(termCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-termCh
		fmt.Println("Ctrl+C pressed")
		ctxCancel()
	}()

	optsFlagSet := flag.NewFlagSet("options", flag.ExitOnError)
	dirFlag := optsFlagSet.String(
		"directory",
		".",
		"An optional directory to which to create files. "+
			"By default, all files and subdirectories are created "+
			"in the current directory.",
	)
	iexCloudBaseURLFlag := optsFlagSet.String(
		"iexcloud-base-url",
		"https://cloud.iexapis.com/stable",
		"IEX Cloud base URL.",
	)
	iexCloudCredentialsFileFlag := optsFlagSet.String(
		"iexcloud-credentials-file",
		".divyield/iexcloud-credentials",
		"IEX Cloud credentials file relative to the user's home directory.",
	)
	dryRunFlag := optsFlagSet.Bool(
		"dry-run",
		false,
		"Show what would be done, without making any changes.",
	)
	dbConnStrFlag := optsFlagSet.String(
		"database",
		"postgres://postgres:postgres@localhost/divyield?sslmode=disable",
		"Database connection string.",
	)
	startDateFlag := optsFlagSet.String(
		"start-date",
		"-11y",
		"Start date of the period, "+
			"format 2010-06-05 or relative -11y.",
	)
	reset := optsFlagSet.Bool(
		"reset",
		false,
		"Reset data in the datebase with the new data",
	)
	noCutDividend := optsFlagSet.Bool(
		"no-cut-dividend",
		false,
		"Dividends were not decreased")

	noDecliningDGR := optsFlagSet.Bool(
		"no-declining-dgr",
		false,
		"no declining DGR")

	divYieldFwdMin := optsFlagSet.Float64(
		"dividend-yield-forward-min",
		0.0,
		"minimum forward dividend yield")

	divYieldFwdMax := optsFlagSet.Float64(
		"dividend-yield-forward-max",
		0.0,
		"maximum forward dividend yield")

	divYieldROIMin := optsFlagSet.Float64(
		"dividend-yield-roi-min",
		0.0,
		"forward dividend yield + DGR-5y average yield "+
			"must be a greater than or equal to the given total yield")

	ggrROIMin := optsFlagSet.Float64(
		"gordon-roi-min",
		10.0,
		"expected return on investment (ROI) "+
			"in the Gordon formula as a percentage")

	ggrMin := optsFlagSet.Float64(
		"gordon-growth-rate-min",
		0.0,
		"minimum Gordon growth rate as a percentage")

	ggrMax := optsFlagSet.Float64(
		"gordon-growth-rate-max",
		0.0,
		"maximum Gordon growth rate as a percentage")

	chartFlag := optsFlagSet.Bool(
		"chart",
		false,
		"generate chart")

	optsFlagSet.Parse(os.Args[2:])

	db, err := sql.Open("postgres", *dbConnStrFlag)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(50)

	pdb := &postgres.DB{
		DB: db,
	}

	startDate, err := parseDate(*startDateFlag)
	if err != nil {
		fmt.Println("invalid start date: ", *startDateFlag)
		os.Exit(1)
	}
	usr, _ := user.Current()
	iexCloudToken, err := ioutil.ReadFile(filepath.Join(
		usr.HomeDir,
		*iexCloudCredentialsFileFlag,
	))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	currencySrv := xrates.NewCurrencyService(
		xrates.RateLimiter(
			rate.NewLimiter(rate.Every(1*time.Second), 1)),
		xrates.Logger(stdoutSync),
	)

	iexc := iexcloud.NewIEXCloud(
		iexcloud.BaseURL(*iexCloudBaseURLFlag),
		iexcloud.Token(string(iexCloudToken)),
		iexcloud.RateLimiter(
			rate.NewLimiter(rate.Every(500*time.Millisecond), 1)),
		iexcloud.Timeout(10*time.Second),
	)
	comProSrv := iexc.NewProfileService()
	isinSrv := iexc.NewISINService()
	exchangeSrv := iexc.NewExchangeService()
	splitSrv := iexc.NewSplitService()
	dividendSrv := iexc.NewDividendService()
	priceSrv := iexc.NewPriceService()

	cmd := cli.NewCommand(
		os.Args[1],
		optsFlagSet.Args(),
		cli.DB(pdb),
		cli.Writer(stdoutSync),
		cli.Dir(*dirFlag),
		cli.DryRun(*dryRunFlag),
		cli.StartDate(startDate),
		cli.Reset(*reset),
		cli.ProfileService(comProSrv),
		cli.ISINService(isinSrv),
		cli.ExchangeService(exchangeSrv),
		cli.SplitService(splitSrv),
		cli.DividendService(dividendSrv),
		cli.PriceService(priceSrv),
		cli.CurrencyService(currencySrv),

		cli.DividendYieldForwardMin(*divYieldFwdMin),
		cli.DividendYieldForwardMax(*divYieldFwdMax),
		cli.DividendYieldTotalMin(*divYieldROIMin),
		cli.GordonROI(*ggrROIMin),
		cli.GordonGrowthRateMin(*ggrMin),
		cli.GordonGrowthRateMax(*ggrMax),
		cli.NoCutDividend(*noCutDividend),
		cli.NoDecliningDGR(*noDecliningDGR),
		cli.Chart(*chartFlag),
	)
	err = cmd.Execute(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var relDateRE *regexp.Regexp = regexp.MustCompile("^-[0-9]+y$")

func parseDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}

	if relDateRE.MatchString(s) {
		nYears, err := strconv.ParseInt(s[1:len(s)-1], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(
			time.Now().UTC().Year()-int(nYears), time.January, 1,
			0, 0, 0, 0, time.UTC,
		), nil
	}

	date, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, err
	}
	return date, nil
}

func parseIEXCloudAPITokens(p string) (map[string]string, error) {
	tokens := make(map[string]string)

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		tokens[record[0]] = strings.TrimSpace(record[1])
	}

	return tokens, nil
}

func subcommandIndex(args []string) int {
	subcommands := []string{"fetch", "chart", "stats"}
	for i, a := range args {
		for _, c := range subcommands {
			if a == c {
				return i
			}
		}
	}
	return len(args)
}

const usage = `usage: divyield <command> [<flags>] [<args>]

Commands:
  fetch		Fetch stock price and dividend history
  stats		Show dividend yield stats
  chart		Create dividend yield chart

See 'divyield <command> -h' to read about a specific command.
`

const usageFetch = `usage: divyield fetch [<flags>] <tickers>

Flags:
  -outputDir string
      output dir (default "work/stocks")
`

const usageStats = `usage: divyield stats [<flags>] <tickers>

Flags:
  -divYieldMin number
      minimum dividend yield
  -divYieldMax number
      maximum dividend yield
`

const usageChart = `usage: divyield chart [<flags>] <tickers>

Flags:
  -endDate string
      end date of the chart period, format 2010-06-05
  -outputDir string
      output dir (default "work/charts")
  -stocksDir string
      stocks dir (default "work/stocks")
`

type StdoutSync struct {
	mu *sync.RWMutex
	w  io.Writer
}

func (l *StdoutSync) Logf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, format, v...)
	fmt.Fprintln(l.w)
}

func (l *StdoutSync) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, err := l.w.Write(p)
	if err != nil {
		return n, err
	}
	return l.w.Write([]byte{'\n'})
}
