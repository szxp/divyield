package fetcher

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"szakszon.com/divyield/logger"
)

type options struct {
	outputDir string
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

func Timeout(d time.Duration) Option {
	return func(o options) options {
		o.timeout = d
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
	startDate: time.Date(1900, time.January, 1, 1, 0, 0, 0, time.UTC),
	endDate:   time.Time{},
	timeout:   0,
	logger:    nil,
}

func NewFetcher(os ...Option) Fetcher {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}
	return Fetcher{
		opts: opts,
	}
}

type Fetcher struct {
	opts options
	errs []error
}

func (f *Fetcher) Fetch(ctx context.Context, tickers []string) {
	if f.opts.endDate.IsZero() {
		f.opts.endDate = time.Now()
	}

	var workerWg sync.WaitGroup
	workerPoolSize := 1
	jobCh := make(chan job)
	var pendingWg sync.WaitGroup
	var resultWg sync.WaitGroup
	resultCh := make(chan result)

	for i := 0; i < workerPoolSize; i++ {
		workerWg.Add(1)
		go func() {
			defer func() {
				workerWg.Done()
			}()
			for job := range jobCh {
				err := f.getStockData(ctx, job.Ticker)
				resultCh <- result{Ticker: job.Ticker, Err: err}
			}
		}()
	}

	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for res := range resultCh {
			if res.Err != nil {
				e := &FetchError{Ticker: res.Ticker, Err: res.Err}
				f.errs = append(f.errs, e)
			} else {
				f.log("%s: %s", res.Ticker, "OK")
			}
			pendingWg.Done()
		}
	}()

LOOP:
	for _, ticker := range tickers {
		ticker := ticker

		select {
		case <-ctx.Done():
			break LOOP
		default:
			// noop
		}

		time.Sleep(time.Second * 1)
		pendingWg.Add(1)
		jobCh <- job{Ticker: ticker}
	}

	pendingWg.Wait()
	close(resultCh)
	resultWg.Wait()

	close(jobCh)
	workerWg.Wait()
}

func (f *Fetcher) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}

type job struct {
	Ticker string
}

type result struct {
	Ticker string
	Err    error
}

func (f *Fetcher) getStockData(ctx context.Context, ticker string) error {
	err := os.MkdirAll(filepath.Join(f.opts.outputDir, ticker), 0666)
	if err != nil {
		return fmt.Errorf("create stock dir: %s", err)
	}
	err = f.getPrices(ctx, ticker)
	if err != nil {
		return fmt.Errorf("download prices: %s", err)
	}
	err = f.getDividends(ctx, ticker)
	if err != nil {
		return fmt.Errorf("download dividends: %s", err)
	}
	return err
}

func (f *Fetcher) getDividends(ctx context.Context, ticker string) error {
	dstPath := filepath.Join(f.opts.outputDir, ticker, "dividends.csv")
	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create dividends file %s: %s", dstPath, err)
	}
	defer dst.Close()

	u := dividendsURL(ticker, f.opts.startDate, f.opts.endDate)
	_, err = f.download(ctx, dst, u)
	if err != nil {
		return fmt.Errorf("download from %s: %s", u, err)
	}

	lines, err := linesN(dstPath, 2)
	if err != nil {
		return fmt.Errorf("read file %s: %s", dstPath, err)
	}

	//	if len(lines) < 2 {
	//		return fmt.Errorf("too few lines: %s", dstPath)
	//	}

	if !strings.HasPrefix(lines[0], "Date,Dividends") {
		return fmt.Errorf("csv header not found: %s", dstPath)
	}

	if len(lines) >= 2 {
		err = sortCSVDesc(dstPath)
		if err != nil {
			return fmt.Errorf("sort %s: %s", dstPath, err)
		}
	}

	return nil
}

func (f *Fetcher) getPrices(ctx context.Context, ticker string) error {
	dstPath := filepath.Join(f.opts.outputDir, ticker, "prices.csv")
	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create prices file %s: %s", dstPath, err)
	}
	defer dst.Close()

	u := pricesURL(ticker, f.opts.startDate, f.opts.endDate)
	_, err = f.download(ctx, dst, u)
	if err != nil {
		return fmt.Errorf("download from %s: %s", u, err)
	}

	lines, err := linesN(dstPath, 2)
	if err != nil {
		return fmt.Errorf("read file %s: %s", dstPath, err)
	}

	if len(lines) < 2 {
		return fmt.Errorf("too few lines: %s", dstPath)
	}

	if !strings.HasPrefix(lines[0], "Date,Open,High,Low,Close,Adj Close,Volume") {
		return fmt.Errorf("csv header not found: %s", dstPath)
	}

	err = sortCSVDesc(dstPath)
	if err != nil {
		return fmt.Errorf("sort %s: %s", dstPath, err)
	}

	return nil
}

func dividendsURL(ticker string, sd, ed time.Time) string {
	return "https://query1.finance.yahoo.com/v7/finance/download/" + ticker +
		"?period1=" + strconv.FormatInt(sd.Unix(), 10) +
		"&period2=" + strconv.FormatInt(ed.Unix(), 10) +
		"&interval=1d&events=div&includeAdjustedClose=true"
}

func pricesURL(ticker string, sd, ed time.Time) string {
	return "https://query1.finance.yahoo.com/v7/finance/download/" + ticker +
		"?period1=" + strconv.FormatInt(sd.Unix(), 10) +
		"&period2=" + strconv.FormatInt(ed.Unix(), 10) +
		"&interval=1d&events=history&includeAdjustedClose=true"
}

func (f *Fetcher) download(ctx context.Context, dst io.Writer, u string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{
		Timeout: f.opts.timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return 0, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	return io.Copy(dst, resp.Body)
}

func (f *Fetcher) Errs() []error {
	return f.errs
}

type FetchError struct {
	Ticker string
	Err    error
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("%s: %s", e.Ticker, e.Err)
}

func linesN(path string, n int) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string
	for scanner.Scan() {
		if n == 0 {
			break
		}
		text = append(text, scanner.Text())
		if n > 0 {
			n -= 1
		}
	}
	return text, nil
}

func sortCSVDesc(p string) error {
	lines, err := linesN(p, -1)
	if err != nil {
		return err
	}
	if len(lines) < 3 {
		return nil
	}

	header := lines[0]
	body := lines[1:]
	sort.Sort(sort.Reverse(sort.StringSlice(body)))

	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	_, err = w.WriteString(header)
	if err != nil {
		return err
	}
	err = w.WriteByte('\n')
	if err != nil {
		return err
	}

	for _, line := range body {
		_, err = w.WriteString(line)
		if err != nil {
			return err
		}
		err = w.WriteByte('\n')
		if err != nil {
			return err
		}
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	return err
}
