package fetcher

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"io/ioutil"
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

	downloadFrom := time.Date(time.Now().Year()-5, time.January, 1, 1, 0, 0, 0, time.UTC)
	if len(savedDividends) > 0 {
		downloadFrom = time.Time(savedDividends[0].ExDate)
	}

	now := time.Now().UTC()
	nowDate := timeDate(now)

	if downloadFrom.Equal(nowDate) {
		f.log("%s: %s", ticker, "up to date, skip download")
		return nil // up-to-date
	}

	newDividends, err := f.downloadDividends(ctx, ticker, downloadFrom, f.opts.iexCloudAPIToken)
	if err != nil {
		return fmt.Errorf("download dividends %s: %s", ticker, err)
	}

	if len(newDividends) > 1 {
		f.log("%s: %d new dividends", ticker, len(newDividends)-1)
	}

	// new dividends and saved dividends must overlap
	if len(newDividends) > 0 && len(savedDividends) > 0 {
		newBottom := newDividends[len(newDividends)-1]
		savedTop := savedDividends[0]
		if newBottom.Refid != savedTop.Refid {
			return fmt.Errorf("non-overlapping refids %v vs %v", newBottom.Refid, savedTop.Refid)
		}
	}

	mergedDividends := make([]*dividend, 0, len(newDividends)+len(savedDividends))
	mergedDividends = append(mergedDividends, newDividends...)
	if len(savedDividends) > 0 {
		mergedDividends = append(mergedDividends, savedDividends[1:]...)
	}

	err = f.saveDividends(p, mergedDividends)
	if err != nil {
		return fmt.Errorf("save dividends %s: %s", p, err)
	}
	return nil
}

func timeDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
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

	dividends, err := f.parseDividends(ph)
	if err != nil {
		return nil, fmt.Errorf("parse dividends: %s", err)
	}

	sortDividendsDesc(dividends)
	return dividends, nil
}

func (f *Fetcher) parseDividends(r io.Reader) ([]*dividend, error) {
	dividends := make([]*dividend, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var v dividend
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf("decode: %s", err)
		}
		dividends = append(dividends, &v)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	return dividends, nil
}

func sortDividendsDesc(dividends []*dividend) {
	sort.SliceStable(dividends, func(i, j int) bool {
		ti := time.Time(dividends[i].ExDate)
		tj := time.Time(dividends[j].ExDate)
		return ti.After(tj)
	})
}

func (f *Fetcher) downloadDividends(ctx context.Context, ticker string, from time.Time, apiToken string) ([]*dividend, error) {
	u := dividendsURL(ticker, from, apiToken)
	//fmt.Println(u)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	dividends, err := f.parseDividends(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse dividends: %s", err)
	}

	sortDividendsDesc(dividends)
	return dividends, nil
}

func dividendsURL(ticker string, from time.Time, apiToken string) string {
	return "https://cloud.iexapis.com/stable/time-series/DIVIDENDS/" + ticker +
		"?from=" + from.Format(timeFormat) +
		"&token=" + apiToken

	// return "https://query1.finance.yahoo.com/v7/finance/download/" + ticker +
	// 	"?period1=" + strconv.FormatInt(sd.Unix(), 10) +
	// 	"&period2=" + strconv.FormatInt(ed.Unix(), 10) +
	// 	"&interval=1d&events=div&includeAdjustedClose=true"
}

func (f *Fetcher) saveDividends(p string, dividends []*dividend) error {
	tmp, err := f.saveJsonTmp(filepath.Dir(p), "dividends.tmp.json", dividends)
	if err != nil {
		return fmt.Errorf("save temp dividends: %s", err)
	}
	defer os.Remove(tmp)

	if err = os.Rename(tmp, p); err != nil {
		return fmt.Errorf("rename %s -> %s: %s", tmp, p, err)
	}

	return nil
}

func (f *Fetcher) saveJsonTmp(dir, name string, v interface{}) (string, error) {
	tmp, err := ioutil.TempFile(dir, name)
	if err != nil {
		return "", fmt.Errorf("create temp file: %s", err)
	}
	defer tmp.Close()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "    ")
	if err = enc.Encode(v); err != nil {
		return "", fmt.Errorf("encode: %s", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("create temp file: %s", err)
	}

	return tmp.Name(), nil
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

func (t Time) Equal(o Time) bool {
	return time.Time(t).Equal(time.Time(o))
}

func (t Time) String() string {
	st := time.Time(t)
	if st.IsZero() {
		return "0000-00-00"
	}
	return st.Format(timeFormat)
}

func (t Time) MarshalJSON() ([]byte, error) {
	st := time.Time(t)
	if st.IsZero() {
		return []byte("\"0000-00-00\""), nil
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

func (t TimeUnix) Equal(o TimeUnix) bool {
	return time.Time(t).Equal(time.Time(o))
}

func (t TimeUnix) String() string {
	st := time.Time(t)
	if st.IsZero() {
		return "0"
	}
	return strconv.FormatInt(time.Time(t).Unix()*1000, 10)
}

func (t TimeUnix) MarshalJSON() ([]byte, error) {
	st := time.Time(t)
	if st.IsZero() {
		return []byte("0"), nil
	}
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
	p := filepath.Join(f.opts.outputDir, ticker, "prices.json")

	savedPrices, err := f.loadPrices(p)
	if err != nil {
		return fmt.Errorf("load prices file %s: %s", p, err)
	}

	downloadFrom := time.Date(time.Now().Year()-5, time.January, 1, 1, 0, 0, 0, time.UTC)
	if len(savedPrices) > 0 {
		downloadFrom = time.Time(savedPrices[0].Date)
	}

	now := time.Now().UTC()
	nowDate := timeDate(now)

	if downloadFrom.Equal(nowDate) {
		f.log("%s: %s", ticker, "up to date, skip download")
		return nil // up-to-date
	}

	newPrices, err := f.downloadPrices(ctx, ticker, downloadFrom, f.opts.iexCloudAPIToken)
	if err != nil {
		return fmt.Errorf("download prices %s: %s", ticker, err)
	}

	if len(newPrices) > 1 {
		f.log("%s: %d new prices", ticker, len(newPrices)-1)
	}

	// new prices and saved prices must overlap
	if len(newPrices) > 0 && len(savedPrices) > 0 {
		newBottom := newPrices[len(newPrices)-1]
		savedTop := savedPrices[0]
		if !newBottom.Date.Equal(savedTop.Date) {
			return fmt.Errorf("non-overlapping price dates %v vs %v", newBottom.Date, savedTop.Date)
		}
	}

	mergedPrices := make([]*price, 0, len(newPrices)+len(savedPrices))
	mergedPrices = append(mergedPrices, newPrices...)
	if len(savedPrices) > 0 {
		mergedPrices = append(mergedPrices, savedPrices[1:]...)
	}

	err = f.savePrices(p, mergedPrices)
	if err != nil {
		return fmt.Errorf("save prices %s: %s", p, err)
	}
	return nil
}

func (f *Fetcher) loadPrices(p string) ([]*price, error) {
	ph, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []*price{}, nil
		}
		return nil, fmt.Errorf("open prices file %s: %s", p, err)
	}
	defer ph.Close()

	prices, err := f.parsePrices(ph)
	if err != nil {
		return nil, fmt.Errorf("parse prices: %s", err)
	}

	sortPricesDesc(prices)
	return prices, nil
}

func (f *Fetcher) parsePrices(r io.Reader) ([]*price, error) {
	prices := make([]*price, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var v price
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf("decode: %s", err)
		}
		prices = append(prices, &v)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	return prices, nil
}

func sortPricesDesc(prices []*price) {
	sort.SliceStable(prices, func(i, j int) bool {
		ti := time.Time(prices[i].Date)
		tj := time.Time(prices[j].Date)
		return ti.After(tj)
	})
}

func (f *Fetcher) downloadPrices(ctx context.Context, ticker string, from time.Time, apiToken string) ([]*price, error) {
	u := pricesURL(ticker, from, apiToken)
	//fmt.Println(u)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	prices, err := f.parsePrices(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse prices: %s", err)
	}

	sortPricesDesc(prices)
	return prices, nil
}

func pricesURL(ticker string, from time.Time, apiToken string) string {
	return "https://cloud.iexapis.com/stable/time-series/HISTORICAL_PRICES/" + ticker +
		"?from=" + from.Format(timeFormat) +
		"&token=" + apiToken

	// return "https://query1.finance.yahoo.com/v7/finance/download/" + ticker +
	// 	"?period1=" + strconv.FormatInt(sd.Unix(), 10) +
	// 	"&period2=" + strconv.FormatInt(ed.Unix(), 10) +
	// 	"&interval=1d&events=history&includeAdjustedClose=true"
}

func (f *Fetcher) savePrices(p string, prices []*price) error {
	tmp, err := f.saveJsonTmp(filepath.Dir(p), "prices.tmp.json", prices)
	if err != nil {
		return fmt.Errorf("save temp prices: %s", err)
	}
	defer os.Remove(tmp)

	if err = os.Rename(tmp, p); err != nil {
		return fmt.Errorf("rename %s -> %s: %s", tmp, p, err)
	}

	return nil
}

type price struct {
	Close   float64  `json:"close"`
	Fclose  float64  `json:"fclose"`
	Fhigh   float64  `json:"fhigh"`
	Flow    float64  `json:"flow"`
	Fopen   float64  `json:"fopen"`
	Fvolume float64  `json:"fvolume"`
	High    float64  `json:"high"`
	Low     float64  `json:"low"`
	Open    float64  `json:"open"`
	Symbol  string   `json:"symbol"`
	Uclose  float64  `json:"uclose"`
	Uhigh   float64  `json:"uhigh"`
	Ulow    float64  `json:"ulow"`
	Uopen   float64  `json:"uopen"`
	Uvolume float64  `json:"uvolume"`
	Volume  float64  `json:"volume"`
	ID      string   `json:"id"`
	Key     string   `json:"key"`
	SubKey  string   `json:"subkey"`
	Date    TimeUnix `json:"date"`
	Updated TimeUnix `json:"updated"`
}

func (p *price) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(p.Date).Format(timeFormat),
		p.Fclose,
	)
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
