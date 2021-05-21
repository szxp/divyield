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

	//"szakszon.com/divyield"
	"szakszon.com/divyield/chart"
	"szakszon.com/divyield/iexcloud"
	"szakszon.com/divyield/postgres"
	"szakszon.com/divyield/stats"
	//"szakszon.com/divyield/xrates"
	"szakszon.com/divyield/cli"
)

const defaultStocksDir = "work/stocks"
const defaultChartOutputDir = "work/charts"

func main() {
	var err error
	ctx := context.Background()
	ctx, ctxCancel := context.WithCancel(ctx)

	now := time.Now()
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

	noCutDividend := flag.CommandLine.Bool(
		"no-cut-dividend",
		false,
		"Dividends were not decreased")

	noDecliningDGR := flag.CommandLine.Bool(
		"no-declining-dgr",
		false,
		"no declining DGR")

	divYieldFwdMin := flag.CommandLine.Float64(
		"dividend-yield-forward-min",
		0.0,
		"minimum forward dividend yield")

	divYieldFwdMax := flag.CommandLine.Float64(
		"dividend-yield-forward-max",
		0.0,
		"maximum forward dividend yield")

	divYieldROIMin := flag.CommandLine.Float64(
		"dividend-yield-roi-min",
		0.0,
		"forward dividend yield + DGR-5y average yield "+
			"must be a greater than or equal to the given total yield")

	ggrROIMin := flag.CommandLine.Float64(
		"gordon-roi-min",
		10.0,
		"expected return on investment (ROI) "+
			"in the Gordon formula as a percentage")

	ggrMin := flag.CommandLine.Float64(
		"gordon-growth-rate-min",
		0.0,
		"minimum Gordon growth rate as a percentage")

	ggrMax := flag.CommandLine.Float64(
		"gordon-growth-rate-max",
		0.0,
		"maximum Gordon growth rate as a percentage")

	//	chartFlag := flag.CommandLine.Bool(
	//		"chart",
	//		false,
	//		"generate chart")

	chartOutputDir := flag.CommandLine.String(
		"chart-output-dir",
		defaultChartOutputDir,
		"chart output dir")

	fetchCmd := flag.NewFlagSet("fetch", flag.ExitOnError)
	fetchCmd.Usage = func() {
		fmt.Println(usageFetch)
		os.Exit(1)
	}
	fetchOutputDir := fetchCmd.String("outputDir",
		defaultStocksDir, "output dir")
	fetchForce := fetchCmd.Bool("force", false,
		"force downloading stock data even if it is already downloaded")
	fetchIEXCloudAPITokensFile := fetchCmd.String("iexCloudAPITokens", "",
		"IEXCloud API Token csv file")

	statsCmd := flag.NewFlagSet("stats", flag.ExitOnError)
	statsCmd.Usage = func() {
		fmt.Println(usageStats)
		os.Exit(1)
	}
	statsStocksDir := statsCmd.String("stocksDir",
		defaultStocksDir, "stocks dir")

	chartCmd := flag.NewFlagSet("chart", flag.ExitOnError)
	chartCmd.Usage = func() {
		fmt.Println(usageChart)
		os.Exit(1)
	}

	if len(os.Args) < 2 {
		fmt.Println(usage)
		os.Exit(1)
	}

	subIdx := subcommandIndex(os.Args)
	err = flag.CommandLine.Parse(os.Args[1:subIdx])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)

	}

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
		"-10y",
		"Start date of the period, "+
			"format 2010-06-05 or relative -10y.",
	)
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

	iexc := iexcloud.NewIEXCloud(
		iexcloud.BaseURL(*iexCloudBaseURLFlag),
		iexcloud.Token(string(iexCloudToken)),
		iexcloud.RateLimiter(
			rate.NewLimiter(rate.Every(500*time.Millisecond), 1)),
	)
	comProSrv := iexc.NewCompanyProfileService()
	isinSrv := iexc.NewISINService()
	exchangeSrv := iexc.NewExchangeService()
	splitSrv := iexc.NewSplitService()

	cmd := cli.NewCommand(
		os.Args[1],
		optsFlagSet.Args(),
		cli.DB(pdb),
		cli.Writer(stdoutSync),
		cli.Dir(*dirFlag),
		cli.DryRun(*dryRunFlag),
		cli.StartDate(startDate),
		cli.CompanyProfileService(comProSrv),
		cli.ISINService(isinSrv),
		cli.ExchangeService(exchangeSrv),
		cli.SplitService(splitSrv),
	)
	err = cmd.Execute(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return

	/*
			cc := xrates.NewCurrencyConverter(
				xrates.Logger(stdoutSync),
			)
			ccin := &divyield.CurrencyConvertInput{
				From:   "CAD",
				To:     "USD",
				Amount: 26.00,
				Date:   time.Date(2021, time.May, 18, 0, 0, 0, 0, time.UTC),
			}
			ccout, err := cc.Convert(ctx, ccin)
		    if err != nil {
				fmt.Println(err)
				return
		    }
			stdoutSync.Logf("%f %f%%", ccout.Amount, ccout.Rate)
	*/

	switch os.Args[subIdx] {
	case "fetch":
		fetchCmd.Parse(os.Args[subIdx+1:])

		tickers := fetchCmd.Args()
		if len(tickers) == 0 {
			fmt.Println("tickers not specified")
			return
		}

		iexCloudAPITokens, err := parseIEXCloudAPITokens(
			*fetchIEXCloudAPITokensFile)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fetcher := iexcloud.NewStockFetcher(
			iexcloud.OutputDir(*fetchOutputDir),
			iexcloud.Workers(2),
			iexcloud.RateLimiter(rate.NewLimiter(rate.Every(500*time.Millisecond), 2)),
			iexcloud.Timeout(10*time.Second),
			iexcloud.IEXCloudAPITokens(iexCloudAPITokens),
			iexcloud.Force(*fetchForce),
			iexcloud.Log(stdoutSync),
			iexcloud.DB(pdb),
		)
		fetcher.Fetch(ctx, tickers)
		for _, err := range fetcher.Errs() {
			fmt.Println("Error:", err)
		}

	case "stats":
		statsCmd.Parse(os.Args[subIdx+1:])

		tickers := statsCmd.Args()
		if len(tickers) == 0 {
			fmt.Println("tickers not specified")
			return
		}

		statsGenerator := stats.NewStatsGenerator(
			stats.StocksDir(*statsStocksDir),
			stats.Now(now),
			stats.Log(stdoutSync),
			stats.DB(pdb),
			stats.StartDate(startDate),
			stats.DividendYieldForwardMin(*divYieldFwdMin),
			stats.DividendYieldForwardMax(*divYieldFwdMax),
			stats.DividendYieldTotalMin(*divYieldROIMin),
			stats.GordonROI(*ggrROIMin),
			stats.GordonGrowthRateMin(*ggrMin),
			stats.GordonGrowthRateMax(*ggrMax),
			stats.NoCutDividend(*noCutDividend),
			stats.NoDecliningDGR(*noDecliningDGR),
		)
		stats, err := statsGenerator.Generate(ctx, tickers)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println(stats)

	case "chart":
		chartCmd.Parse(os.Args[subIdx+1:])

		tickers := chartCmd.Args()
		if len(tickers) == 0 {
			fmt.Println("tickers not specified")
			return
		}

		chartGener := chart.NewChartGenerator(
			chart.OutputDir(*chartOutputDir),
			chart.StartDate(startDate),
			chart.Log(stdoutSync),
			chart.DB(pdb),
		)
		err = chartGener.Generate(ctx, tickers)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	default:
		fmt.Println(usage)
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
