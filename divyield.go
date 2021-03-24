package main

import (
	"flag"
	"fmt"
	"regexp"
	"time"
	"os"
	"os/signal"
	"syscall"
	"context"
	"strconv"

	"szakszon.com/divyield/fetcher"
	"szakszon.com/divyield/stats"
	"szakszon.com/divyield/charter"
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

	fetchCmd := flag.NewFlagSet("fetch", flag.ExitOnError)
	fetchCmd.Usage = func() {
		fmt.Println(usageFetch)
		os.Exit(1) 
	}
	fetchOutputDir := fetchCmd.String("outputDir", defaultStocksDir, "output dir")

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

	switch os.Args[1] {
	case "fetch":
		fetchCmd.Parse(os.Args[2:])

		tickers := fetchCmd.Args()
		if len(tickers) == 0 {
			fmt.Println("tickers not specified")
			return
		}

		fetcher := fetcher.NewFetcher(
			fetcher.OutputDir(*fetchOutputDir),
			fetcher.Timeout(10*time.Second),
			fetcher.Log(&StdoutLogger{}),
		)
		fetcher.Fetch(ctx, tickers)
		for _, err := range fetcher.Errs() {
			fmt.Println("Error:", err)
		}

	case "stats":
		statsCmd.Parse(os.Args[2:])

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
		chartCmd.Parse(os.Args[2:])

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

type StdoutLogger struct {}

func (l *StdoutLogger) Logf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

