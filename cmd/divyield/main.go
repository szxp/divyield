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
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"szakszon.com/divyield/chart"
	"szakszon.com/divyield/iexcloud"
	"szakszon.com/divyield/postgres"
	"szakszon.com/divyield/stats"
	"szakszon.com/divyield/yahoo"
)

var relDateRE *regexp.Regexp = regexp.MustCompile("^-[0-9]+y$")

const defaultStocksDir = "work/stocks"
const defaultChartOutputDir = "work/charts"

func main() {
	var err error
	ctx := context.Background()
	ctx, ctxCancel := context.WithCancel(ctx)

	now := time.Now()
	stdoutLogger := &StdoutLogger{mu: &sync.RWMutex{}}

	termCh := make(chan os.Signal)
	signal.Notify(termCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-termCh
		fmt.Println("Ctrl+C pressed")
		ctxCancel()
	}()

	dbConnStr := flag.CommandLine.String("db",
		"postgres://postgres:postgres@localhost/divyield?sslmode=disable",
		"database connection string")

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
	statsSP500DividendYield := statsCmd.Float64("sp500-dividend-yield",
		0.0, "S&P 500 dividend yield")
	statsSP500MinFactor := statsCmd.Float64("sp500-min-factor",
		1.5, "S&P 500 min factor")
	statsSP500MaxFactor := statsCmd.Float64("sp500-max-factor",
		5.0, "S&P 500 max factor")

	chartCmd := flag.NewFlagSet("chart", flag.ExitOnError)
	chartCmd.Usage = func() {
		fmt.Println(usageChart)
		os.Exit(1)
	}
	chartOutputDir := chartCmd.String("outputDir",
		defaultChartOutputDir, "output dir")
	startDateFlag := chartCmd.String("startDate",
		"-10y", "start date of the chart period, format 2010-06-05 or relative -10y")
	var startDate time.Time

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

	db, err := sql.Open("postgres", *dbConnStr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(50)

	pdb := &postgres.DB{
		DB: db,
	}

	splitFetcher := yahoo.NewSplitFetcher(
		yahoo.RateLimiter(rate.NewLimiter(rate.Every(1*time.Second), 1)),
		yahoo.Timeout(10*time.Second),
		yahoo.Log(stdoutLogger),
	)

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
			iexcloud.Workers(10),
			iexcloud.RateLimiter(rate.NewLimiter(rate.Every(200*time.Millisecond), 1)),
			iexcloud.Timeout(10*time.Second),
			iexcloud.IEXCloudAPITokens(iexCloudAPITokens),
			iexcloud.Force(*fetchForce),
			iexcloud.Log(stdoutLogger),
			iexcloud.DB(pdb),
			iexcloud.SplitFetcher(splitFetcher),
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
			stats.Log(stdoutLogger),
			stats.DB(pdb),
			stats.DividendYieldMin(
				*statsSP500DividendYield**statsSP500MinFactor),
			stats.DividendYieldMax(
				*statsSP500DividendYield**statsSP500MaxFactor),
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

		if *startDateFlag != "" {
			if relDateRE.MatchString(*startDateFlag) {
				nYears, err := strconv.ParseInt(
					(*startDateFlag)[1:len(*startDateFlag)-1], 10, 64)
				if err != nil {
					fmt.Println("invalid start date: ", *startDateFlag)
					return
				}
				startDate = time.Date(
					now.Year()-int(nYears), time.January, 1,
					0, 0, 0, 0, time.UTC,
				)
			} else {
				startDate, err = time.Parse("2006-01-02", *startDateFlag)
				if err != nil {
					fmt.Println("invalid start date: ", *startDateFlag)
					return
				}
			}
		}

		chartGener := chart.NewChartGenerator(
			chart.OutputDir(*chartOutputDir),
			chart.StartDate(startDate),
			chart.Log(stdoutLogger),
			chart.DB(pdb),
		)
		err := chartGener.Generate(ctx, tickers)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	default:
		fmt.Println(usage)
		os.Exit(1)
	}
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
  -startDate string
      start date of the chart period, format 2010-06-05 or relative -10y (default "-10y")
  -stocksDir string
      stocks dir (default "work/stocks")
`

type StdoutLogger struct {
	mu *sync.RWMutex
}

func (l *StdoutLogger) Logf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf(format, v...)
	fmt.Println()
}
