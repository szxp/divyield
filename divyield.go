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

	"szakszon.com/divyield/fetcher"
	"szakszon.com/divyield/charter"
)

var relDateRE *regexp.Regexp = regexp.MustCompile("^-[0-9]+y$")

const defaultStocksDir = "work/stocks"
const defaultChartOutputDir = "work/charts"


func main() {
	ctx := context.Background()
	ctx, ctxCancel := context.WithCancel(ctx)

	termCh := make(chan os.Signal)
	signal.Notify(termCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-termCh
		fmt.Println("Ctrl+C pressed")
		ctxCancel()
	}()

	fetchCmd := flag.NewFlagSet("fetch", flag.ExitOnError)
	fetchOutputDir := fetchCmd.String("outputDir", defaultStocksDir, "output dir")

	chartCmd := flag.NewFlagSet("chart", flag.ExitOnError)
	chartOutputDir := chartCmd.String("outputDir", defaultChartOutputDir, "output dir")
	chartStocksDir := chartCmd.String("stocksDir", defaultStocksDir, "stocks dir")
	//startDateFlag := chartCmd.String("startDate", "", "start date of the chart period")
	//endDateFlag := chartCmd.String("endDate", "", "end date of the chart period")

	if len(os.Args) < 2 {
		fmt.Println("expected subcommand, see -help")
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
		fmt.Println("Fetch stock data")
		fetcher.Fetch(ctx, tickers)
		for _, err := range fetcher.Errs() {
			fmt.Println("Error:", err)
		}

	case "chart":
		chartCmd.Parse(os.Args[2:])

		tickers := chartCmd.Args()
		if len(tickers) == 0 {
			fmt.Println("tickers not specified")
			return
		}

		charter := charter.NewCharter(
			charter.OutputDir(*chartOutputDir),
			charter.StocksDir(*chartStocksDir),
			//charter.StartDate(startDate),
			//charter.EndDate(endDate),
			charter.Log(&StdoutLogger{}),
		)
		fmt.Println("Create chart")
		err := charter.Chart(ctx, tickers)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	default:
		fmt.Println("expected subcommand, see -help")
		os.Exit(1)
	}

	/*
	var startDateStr string
	var startDate time.Time
	var endDateStr string
	var endDate time.Time

	flag.StringVar(&startDateStr, "startDate", "", "start date")
	flag.StringVar(&endDateStr, "endDate", "", "end date")
	flag.Parse()

	now := time.Now()

	if startDateStr != "" {
		if relDateRE.MatchString(startDateStr) {
			nYears, err := strconv.ParseInt(startDateStr[1:len(startDateStr)-1], 10, 64)
			if err != nil {
				fmt.Println("invalid start date: ", startDateStr)
				return
			}
			startDate = time.Date(now.Year()-int(nYears), time.January, 1, 0, 0, 0, 0, time.UTC)
		} else {
			startDate, err = time.Parse("2006-01-02", startDateStr)
			if err != nil {
				fmt.Println("invalid start date: ", startDateStr)
				return
			}
		}
	}

	if endDateStr != "" {
		endDate, err = time.Parse("2006-01-02", endDateStr)
		if err != nil {
			fmt.Println("invalid end date: ", endDateStr)
			return
		}
	}

	if flag.NArg() != 1 {
		fmt.Println("specifiy exactly one path to a stock dir")
		return
	}

	ticker := flag.Arg(0)
	stockDir := "stocks/" + ticker
	pricesPath := stockDir + "/prices.csv"
	prices, err := parsePrices(ticker, pricesPath)
	if err != nil {
		fmt.Println("parse prices: %s: %s", pricesPath, err)
		return
	}

	dividendsPath := stockDir + "/dividends.csv"
	dividends, err := parseDividends(dividendsPath)
	if err != nil {
		fmt.Println("parse dividends: %s: %s", dividendsPath, err)
		return
	}
	//fmt.Println(dividends)

	setDividendRecent(prices, dividends)
	printPrices(prices, startDate, endDate)
	*/
}


type StdoutLogger struct {}

func (l *StdoutLogger) Logf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

