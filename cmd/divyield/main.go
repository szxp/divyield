package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"szakszon.com/divyield/charter"
	"szakszon.com/divyield/iexcloud"
	"szakszon.com/divyield/postgres"
	"szakszon.com/divyield/stats"
)

var relDateRE *regexp.Regexp = regexp.MustCompile("^-[0-9]+y$")

const defaultStocksDir = "work/stocks"
const defaultChartOutputDir = "work/charts"

func main() {
	var err error
	ctx := context.Background()
	ctx, ctxCancel := context.WithCancel(ctx)

	now := time.Now()

	termCh := make(chan os.Signal)
	signal.Notify(termCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-termCh
		fmt.Println("Ctrl+C pressed")
		ctxCancel()
	}()

	dbConnStr := flag.CommandLine.String("db", "postgres://postgres:postgres@localhost/divyield?sslmode=disable", "database connection string")

	fetchCmd := flag.NewFlagSet("fetch", flag.ExitOnError)
	fetchCmd.Usage = func() {
		fmt.Println(usageFetch)
		os.Exit(1)
	}
	fetchOutputDir := fetchCmd.String("outputDir", defaultStocksDir, "output dir")
	fetchForce := fetchCmd.Bool("force", false, "force downloading stock data even if it is already downloaded")
	fetchIEXCloudAPIToken := fetchCmd.String("iexCloudAPIToken", "", "IEXCloud API Token, see https://iexcloud.io/docs/api/#authentication")

	statsCmd := flag.NewFlagSet("stats", flag.ExitOnError)
	statsCmd.Usage = func() {
		fmt.Println(usageStats)
		os.Exit(1)
	}
	statsStocksDir := statsCmd.String("stocksDir", defaultStocksDir, "stocks dir")

	chartCmd := flag.NewFlagSet("chart", flag.ExitOnError)
	chartCmd.Usage = func() {
		fmt.Println(usageChart)
		os.Exit(1)
	}
	chartOutputDir := chartCmd.String("outputDir", defaultChartOutputDir, "output dir")
	chartStocksDir := chartCmd.String("stocksDir", defaultStocksDir, "stocks dir")
	startDateFlag := chartCmd.String("startDate", "-10y", "start date of the chart period, format 2010-06-05 or relative -10y")
	endDateFlag := chartCmd.String("endDate", "", "end date of the chart period, format 2010-06-05")
	var startDate time.Time
	var endDate time.Time

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

	pdb := &postgres.DB{
		DB: db,
	}

	switch os.Args[subIdx] {
	case "fetch":
		fetchCmd.Parse(os.Args[subIdx+1:])

		tickers := fetchCmd.Args()
		if len(tickers) == 0 {
			fmt.Println("tickers not specified")
			return
		}

		fetcher := iexcloud.NewStockFetcher(
			iexcloud.OutputDir(*fetchOutputDir),
			iexcloud.Workers(2),
			iexcloud.RateLimiter(rate.NewLimiter(rate.Every(500*time.Millisecond), 1)),
			iexcloud.Timeout(10*time.Second),
			iexcloud.IEXCloudAPIToken(*fetchIEXCloudAPIToken),
			iexcloud.Force(*fetchForce),
			iexcloud.Log(&StdoutLogger{}),
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
			stats.Log(&StdoutLogger{}),
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
				nYears, err := strconv.ParseInt((*startDateFlag)[1:len(*startDateFlag)-1], 10, 64)
				if err != nil {
					fmt.Println("invalid start date: ", *startDateFlag)
					return
				}
				startDate = time.Date(now.Year()-int(nYears), time.January, 1, 0, 0, 0, 0, time.UTC)
			} else {
				startDate, err = time.Parse("2006-01-02", *startDateFlag)
				if err != nil {
					fmt.Println("invalid start date: ", *startDateFlag)
					return
				}
			}
		}

		if *endDateFlag == "" {
			endDate = now
		} else {
			endDate, err = time.Parse("2006-01-02", *endDateFlag)
			if err != nil {
				fmt.Println("invalid end date: ", *endDateFlag)
				return
			}
		}

		charter := charter.NewCharter(
			charter.OutputDir(*chartOutputDir),
			charter.StocksDir(*chartStocksDir),
			charter.StartDate(startDate),
			charter.EndDate(endDate),
			charter.Log(&StdoutLogger{}),
		)
		err := charter.Chart(ctx, tickers)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	default:
		fmt.Println(usage)
		os.Exit(1)
	}
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

type StdoutLogger struct{}

func (l *StdoutLogger) Logf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}