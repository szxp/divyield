package fetcher

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
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
	outputDir        string
	startDate        time.Time
	endDate          time.Time
	timeout          time.Duration // http client timeout, 0 means no timeout
	iexCloudAPIToken string
	force            bool
	logger           logger.Logger
	rateLimiter      *rate.Limiter
	workers          int
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

func Workers(n int) Option {
	return func(o options) options {
		o.workers = n
		return o
	}
}

func RateLimiter(rl *rate.Limiter) Option {
	return func(o options) options {
		o.rateLimiter = rl
		return o
	}
}

func Timeout(d time.Duration) Option {
	return func(o options) options {
		o.timeout = d
		return o
	}
}

func IEXCloudAPIToken(t string) Option {
	return func(o options) options {
		o.iexCloudAPIToken = t
		return o
	}
}

func Force(f bool) Option {
	return func(o options) options {
		o.force = f
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
	outputDir:   "",
	startDate:   time.Date(1900, time.January, 1, 1, 0, 0, 0, time.UTC),
	endDate:     time.Time{},
	workers:     1,
	rateLimiter: rate.NewLimiter(rate.Every(1*time.Second), 1),
	timeout:     0,
	logger:      nil,
}

type RLClient struct {
	client      *http.Client
	ratelimiter *rate.Limiter
}

func (c *RLClient) Do(req *http.Request) (*http.Response, error) {
	err := c.ratelimiter.Wait(req.Context())
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func NewFetcher(os ...Option) Fetcher {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	return Fetcher{
		client: &RLClient{
			client: &http.Client{
				Timeout: opts.timeout,
			},
			ratelimiter: opts.rateLimiter,
		},
		opts: opts,
	}
}

type Fetcher struct {
	client *RLClient
	opts   options
	errs   []error
}

func (f *Fetcher) Fetch(ctx context.Context, tickers []string) {
	if f.opts.endDate.IsZero() {
		f.opts.endDate = time.Now()
	}

	var workerWg sync.WaitGroup
	jobCh := make(chan job)
	var pendingWg sync.WaitGroup
	var resultWg sync.WaitGroup
	resultCh := make(chan result)

	for i := 0; i < f.opts.workers; i++ {
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
	p := filepath.Join(f.opts.outputDir, ticker, "dividends.json")

	savedDividends, err := f.loadDividends(p)
	if err != nil {
		return fmt.Errorf("load dividends file %s: %s", p, err)
	}

	elapsedDays := int64(-1)
	if len(savedDividends) > 0 {
		elapsedDays = savedDividends[0].ExDate.UntilDays(time.Now())
	}
	
	downloadPeriod := downloadPeriod(elapsedDays)
	if downloadPeriod == "" {
		return nil // up-to-date, ready
	}



	return nil

	// ph, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	// if err != nil {
	// 	return fmt.Errorf("create dividends file %s: %s", p, err)
	// }
	// defer ph.Close()

	// u := dividendsURL(ticker, f.opts.startDate, f.opts.endDate)
	// _, err = f.download(ctx, ph, u)
	// if err != nil {
	// 	return fmt.Errorf("download from %s: %s", u, err)
	// }

	// lines, err := linesN(p, 2)
	// if err != nil {
	// 	return fmt.Errorf("read file %s: %s", p, err)
	// }

	// //	if len(lines) < 2 {
	// //		return fmt.Errorf("too few lines: %s", p)
	// //	}

	// if !strings.HasPrefix(lines[0], "Date,Dividends") {
	// 	return fmt.Errorf("csv header not found: %s", p)
	// }

	// if len(lines) >= 2 {
	// 	err = sortCSVDesc(p)
	// 	if err != nil {
	// 		return fmt.Errorf("sort %s: %s", p, err)
	// 	}
	// }

	return nil
}

func downloadPeriod(days int64) string {
	if days < 0 {
		return "5y"
	} else if days == 0 {
		return ""
	} else if days <= 27 {
		return "1m"		
	} else if days <= 85 {
		return "3m"
	} else if days <= 180 {
		return "6m"
	} else if days <= 360 {
		return "1y"
	} else if days <= 725 {
		return "2y"
	}
	return "5y"
}

func (f *Fetcher) loadDividends(p string) ([]*dividend, error) {
	ph, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []*dividend{}, nil
		}
		return nil, fmt.Errorf("open dividends file %s: %s", p, err)
	}
	defer ph.Close()

	dividends := make([]*dividend, 0)

	dec := json.NewDecoder(ph)
	// read open bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var d dividend
		err := dec.Decode(&d)
		if err != nil {
			return nil, fmt.Errorf("decode: %s", err)
		}
		dividends = append(dividends, &d)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	return dividends, nil
}

type dividend struct {
	Amount       float64  `json:"amount"`
	Currency     string   `json:"currency"`
	DeclaredDate Time     `json:"declaredDate"`
	Description  string   `json:"description"`
	ExDate       Time     `json:"exDate"`
	Flag         string   `json:"flag"`
	Frequency    string   `json:"frequency"`
	PaymentDate  Time     `json:"paymentDate"`
	RecordDate   Time     `json:"recordDate"`
	Refid        int64    `json:"refid"`
	Symbol       string   `json:"symbol"`
	ID           string   `json:"id"`
	Key          string   `json:"key"`
	SubKey       string   `json:"subkey"`
	Date         TimeUnix `json:"date"`
	Updated      TimeUnix `json:"updated"`
}

func (d *dividend) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(d.ExDate).Format(timeFormat),
		d.Amount,
	)
}

type Time time.Time

const timeFormat = "2006-01-02"

func (t Time) UntilDays(p time.Time) int64 {
	st := time.Time(t)
	if st.IsZero() {
		return -1
	}
	return int64(p.Sub(st) / (24 * time.Hour))
}

func (t Time) MarshalJSON() ([]byte, error) {
	st := time.Time(t)
	if st.IsZero() {
		return []byte("0000-00-00"), nil
	}
	s := fmt.Sprintf("%q", st.Format(timeFormat))
	return []byte(s), nil
}

func (t *Time) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "0000-00-00" {
		*t = Time(time.Time{})
		return nil
	}

	st, err := time.Parse(timeFormat, s)
	if err != nil {
		return err
	}
	*t = Time(st)
	return nil
}

type TimeUnix time.Time

func (t TimeUnix) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).Unix() * 1000)
}

func (t *TimeUnix) UnmarshalJSON(b []byte) error {
	var i int64
	if err := json.Unmarshal(b, &i); err != nil {
		return err
	}
	*t = TimeUnix(time.Unix(i/1000, 0))
	return nil
}

func (f *Fetcher) getPrices(ctx context.Context, ticker string) error {
	p := filepath.Join(f.opts.outputDir, ticker, "prices.csv")
	ph, err := os.Create(p)
	if err != nil {
		return fmt.Errorf("create prices file %s: %s", p, err)
	}
	defer ph.Close()

	u := pricesURL(ticker, f.opts.startDate, f.opts.endDate)
	_, err = f.download(ctx, ph, u)
	if err != nil {
		return fmt.Errorf("download from %s: %s", u, err)
	}

	lines, err := linesN(p, 2)
	if err != nil {
		return fmt.Errorf("read file %s: %s", p, err)
	}

	if len(lines) < 2 {
		return fmt.Errorf("too few lines: %s", p)
	}

	if !strings.HasPrefix(lines[0], "Date,Open,High,Low,Close,Adj Close,Volume") {
		return fmt.Errorf("csv header not found: %s", p)
	}

	err = sortCSVDesc(p)
	if err != nil {
		return fmt.Errorf("sort %s: %s", p, err)
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
	resp, err := f.client.Do(req)
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
