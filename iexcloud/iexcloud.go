package iexcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"szakszon.com/divyield"
	"szakszon.com/divyield/httprate"
	"szakszon.com/divyield/logger"
)

type IEXCloud struct {
	opts       options
	httpClient *httprate.RLClient
}

func NewIEXCloud(os ...Option) *IEXCloud {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	httpClient := &httprate.RLClient{
		Client: &http.Client{
			Timeout: opts.timeout,
		},
		Ratelimiter: opts.rateLimiter,
	}

	return &IEXCloud{
		opts:       opts,
		httpClient: httpClient,
	}
}

func (c *IEXCloud) companyURL(symbol string) string {
	symbol = strings.ToLower(symbol)
	return c.opts.baseURL +
		"/stock/" + symbol + "/company" +
		"?token=" + c.opts.token
}

func (c *IEXCloud) isinMappingURL(isin string) string {
	return c.opts.baseURL +
		"/ref-data/isin" +
		"?isin=" + isin +
		"&token=" + c.opts.token
}

func (c *IEXCloud) internationalExchangesURL() string {
	return c.opts.baseURL +
		"/ref-data/exchanges" +
		"?token=" + c.opts.token
}

func (c *IEXCloud) splitsURL(
	symbol string,
	from time.Time,
) string {
	symbol = strings.ToLower(symbol)
	return c.opts.baseURL +
		"/time-series" +
		"/SPLITS/" + symbol +
		"?from=" + from.Format(divyield.DateFormat) +
		"&sort=DESC" +
		"&token=" + c.opts.token
}

func (c *IEXCloud) dividendsURL(
	symbol string,
	from time.Time,
) string {
	symbol = strings.ToLower(symbol)
	return c.opts.baseURL +
		"/time-series" +
		"/DIVIDENDS/" + symbol +
		"?from=" + from.Format(divyield.DateFormat) +
		"&sort=DESC" +
		"&token=" + c.opts.token
}

func (c *IEXCloud) pricesURL(
	symbol string,
	from time.Time,
) string {
	symbol = strings.ToLower(symbol)
	return c.opts.baseURL +
		"/time-series" +
		"/HISTORICAL_PRICES/" + symbol +
		"?from=" + from.Format(divyield.DateFormat) +
		"&sort=DESC" +
		"&token=" + c.opts.token
}

func (c *IEXCloud) httpGet(
	ctx context.Context,
	u string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	return c.httpClient.Do(req)
}

func (c *IEXCloud) NewPriceService() divyield.PriceService {
	return &priceService{
		IEXCloud: c,
	}
}

type priceService struct {
	*IEXCloud
}

func (s *priceService) Fetch(
	ctx context.Context,
	in *divyield.PriceFetchInput,
) (*divyield.PriceFetchOutput, error) {
	u := s.pricesURL(in.Symbol, in.From)
	resp, err := s.httpGet(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fmt.Printf("%v: %v %v\n", in.Symbol, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	prices, err := s.parsePrices(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse prices: %s", err)
	}
	sortPricesDesc(prices)

	out := &divyield.PriceFetchOutput{
		Prices: make([]*divyield.Price, 0, len(prices)),
	}

	for _, v := range prices {
		price := &divyield.Price{
			Date:   time.Time(v.Date),
			Symbol: v.Symbol,
			Close:  v.UClose,
			High:   v.UHigh,
			Low:    v.ULow,
			Open:   v.UOpen,
			Volume: v.UVolume,
		}
		out.Prices = append(out.Prices, price)
	}

	return out, nil
}

func (s *priceService) parsePrices(r io.Reader) ([]*price, error) {
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

func sortPricesDesc(a []*price) {
	sort.SliceStable(a, func(i, j int) bool {
		ti := time.Time(a[i].Date)
		tj := time.Time(a[j].Date)
		return ti.After(tj)
	})
}

type price struct {
	Date    timeUnix `json:"date"`
	Symbol  string   `json:"symbol"`
	UClose  float64  `json:"uClose"`
	UHigh   float64  `json:"uHigh"`
	ULow    float64  `json:"uLow"`
	UOpen   float64  `json:"uOpen"`
	UVolume float64  `json:"uVolume"`
}

func (p *price) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(p.Date).Format(DateFormat),
		p.UClose,
	)
}

func (c *IEXCloud) NewDividendService() divyield.DividendService {
	return &dividendService{
		IEXCloud: c,
	}
}

type dividendService struct {
	*IEXCloud
}

func (s *dividendService) Fetch(
	ctx context.Context,
	in *divyield.DividendFetchInput,
) (*divyield.DividendFetchOutput, error) {
	u := s.dividendsURL(in.Symbol, in.From)
	resp, err := s.httpGet(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fmt.Printf("%v: %v %v\n", in.Symbol, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	dividends, err := s.parseDividends(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse dividends: %s", err)
	}
	sortDividendsDesc(dividends)

	out := &divyield.DividendFetchOutput{
		Dividends: make([]*divyield.Dividend, 0, len(dividends)),
	}

	for _, v := range dividends {
		dividend := &divyield.Dividend{
			ID:          v.Refid,
			ExDate:      time.Time(v.ExDate),
			Symbol:      v.Symbol,
			Amount:      v.Amount,
			Currency:    v.Currency,
			Frequency:   v.FrequencyNumber(),
			PaymentType: v.Flag,
		}
		out.Dividends = append(out.Dividends, dividend)
	}

	return out, nil
}

func (s *dividendService) parseDividends(r io.Reader) ([]*dividend, error) {
	dividends := make([]*dividend, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	processed := make(map[int64]struct{})

	// while the array contains values
	for dec.More() {
		var v dividend
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf("decode: %s", err)
		}

		// skip future dividend dates
		if v.ExDate.After(time.Now().UTC()) {
			continue
		}

		if _, ok := processed[v.Refid]; !ok {
			dividends = append(dividends, &v)
			processed[v.Refid] = struct{}{}
		}
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	return dividends, nil
}

func sortDividendsDesc(a []*dividend) {
	sort.SliceStable(a, func(i, j int) bool {
		ti := time.Time(a[i].ExDate)
		tj := time.Time(a[j].ExDate)
		return ti.After(tj)
	})
}

type dividend struct {
	ExDate      date    `json:"exDate"`
	PaymentDate date    `json:"paymentDate"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Flag        string  `json:"flag"`
	Frequency   string  `json:"frequency"`
	Refid       int64   `json:"refid"`
	Symbol      string  `json:"symbol"`
}

func (d *dividend) String() string {
	return fmt.Sprintf("%v: %v (refid %v)",
		time.Time(d.ExDate).Format(DateFormat),
		d.Amount,
		d.Refid,
	)
}

func (d *dividend) FrequencyNumber() int {
	if d.Frequency == "monthly" {
		return 12
	}
	if d.Frequency == "quarterly" {
		return 4
	}
	if d.Frequency == "semi-annual" {
		return 2
	}
	if d.Frequency == "annual" {
		return 1
	}
	if d.Frequency == "blank" ||
		d.Frequency == "unspecified" ||
		d.Frequency == "irregular" {
		return 0
	}

	if d.Symbol == "R" && d.Frequency == "weekly" {
		d.Frequency = "quarterly"
		return 4 // quarterly, fix data error
	}

	panic(fmt.Sprintf("unexpected frequency: %v: %v: %v",
		d.Symbol, d.ExDate, d.Frequency))
}

func (c *IEXCloud) NewSplitService() divyield.SplitService {
	return &splitService{
		IEXCloud: c,
	}
}

type splitService struct {
	*IEXCloud
}

func (s *splitService) Fetch(
	ctx context.Context,
	in *divyield.SplitFetchInput,
) (*divyield.SplitFetchOutput, error) {
	u := s.splitsURL(in.Symbol, in.From)
	resp, err := s.httpGet(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fmt.Printf("%v: %v %v\n", in.Symbol, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	splits, err := s.parseSplits(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse splits: %s", err)
	}
	sortSplitsDesc(splits)

	out := &divyield.SplitFetchOutput{
		Splits: make([]*divyield.Split, 0, len(splits)),
	}

	for _, v := range splits {
		split := &divyield.Split{
			ExDate:     time.Time(v.ExDate),
			FromFactor: v.FromFactor,
			ToFactor:   v.ToFactor,
		}
		out.Splits = append(out.Splits, split)
	}

	return out, nil
}

func (c *IEXCloud) parseSplits(
	r io.Reader,
) ([]*split, error) {
	splits := make([]*split, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var v split
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf("decode split: %s", err)
		}
		splits = append(splits, &v)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	return splits, nil
}

func sortSplitsDesc(a []*split) {
	sort.SliceStable(a, func(i, j int) bool {
		ti := time.Time(a[i].ExDate)
		tj := time.Time(a[j].ExDate)
		return ti.After(tj)
	})
}

type split struct {
	ExDate     date `json:"exDate"`
	FromFactor int  `json:"fromFactor"`
	ToFactor   int  `json:"toFactor"`
}

func (c *IEXCloud) NewProfileService() divyield.ProfileService {
	return &profileService{
		IEXCloud: c,
	}
}

type profileService struct {
	*IEXCloud
}

func (s *profileService) Fetch(
	ctx context.Context,
	in *divyield.ProfileFetchInput,
) (*divyield.ProfileFetchOutput, error) {

	u := s.companyURL(in.Symbol)
	resp, err := s.httpGet(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	//fmt.Printf("%v: %v %v\n", in.Symbol, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		if resp.StatusCode == 404 {
			return &divyield.ProfileFetchOutput{}, nil
		}
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	cp, err := s.parseProfile(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse company profile: %s", err)
	}

	address := strings.TrimSpace(cp.Address + " " + cp.Address2)

	comPro := &divyield.Profile{
		Symbol:         cp.Symbol,
		Name:           cp.CompanyName,
		Exchange:       cp.Exchange,
		IssueType:      cp.IssueType,
		Industry:       cp.Industry,
		Sector:         cp.Sector,
		Description:    cp.Description,
		Website:        cp.Website,
		PrimarySicCode: cp.PrimarySicCode,
		Address:        address,
		State:          cp.State,
		City:           cp.City,
		Zip:            cp.Zip,
		Country:        cp.Country,
		Phone:          cp.Phone,
	}

	out := &divyield.ProfileFetchOutput{
		Profile: comPro,
	}
	return out, nil
}

func (c *IEXCloud) parseProfile(
	r io.Reader,
) (*profile, error) {
	dec := json.NewDecoder(r)
	var v profile
	err := dec.Decode(&v)
	if err != nil {
		return nil, fmt.Errorf("decode company profile: %s", err)
	}
	return &v, nil
}

type profile struct {
	Symbol         string `json:"symbol"`
	CompanyName    string `json:"companyName"`
	Exchange       string `json:"exchange"`
	IssueType      string `json:"issueType"`
	Industry       string `json:"industry"`
	Website        string `json:"website"`
	Description    string `json:"description"`
	Sector         string `json:"sector"`
	PrimarySicCode int    `json:"primarySicCode"`
	Address        string `json:"address"`
	Address2       string `json:"address2"`
	City           string `json:"city"`
	Zip            string `json:"zip"`
	State          string `json:"state"`
	Country        string `json:"country"`
	Phone          string `json:"phone"`
}

func (c *IEXCloud) NewISINService() divyield.ISINService {
	return &isinService{
		IEXCloud: c,
	}
}

type isinService struct {
	*IEXCloud
}

func (s *isinService) Resolve(
	ctx context.Context,
	in *divyield.ISINResolveInput,
) (*divyield.ISINResolveOutput, error) {

	u := s.isinMappingURL(in.ISIN)
	resp, err := s.httpGet(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	//fmt.Printf("%v: %v %v\n", in.ISIN, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	symbols, err := s.parseSymbolISINs(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse symbolISINs: %s", err)
	}

	out := &divyield.ISINResolveOutput{
		Symbols: make([]*divyield.SymbolISIN, 0, len(symbols)),
	}

	for _, v := range symbols {
		symbol := &divyield.SymbolISIN{
			Symbol:   v.Symbol,
			Exchange: v.Exchange,
			Region:   v.Region,
		}
		out.Symbols = append(out.Symbols, symbol)
	}

	return out, nil
}

func (c *IEXCloud) parseSymbolISINs(
	r io.Reader,
) ([]*symbolISIN, error) {
	symbols := make([]*symbolISIN, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var v symbolISIN
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf("decode symbolISIN: %s", err)
		}

		symbols = append(symbols, &v)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	sortSymbolsISINs(symbols)
	return symbols, nil
}

func sortSymbolsISINs(symbols []*symbolISIN) {
	sort.SliceStable(symbols, func(i, j int) bool {
		switch strings.Compare(symbols[i].Region, symbols[j].Region) {
		case -1:
			return true
		case 1:
			return false
		}

		switch strings.Compare(symbols[i].Exchange, symbols[j].Exchange) {
		case -1:
			return true
		case 1:
			return false
		}

		return symbols[i].Symbol < symbols[j].Symbol
	})
}

type symbolISIN struct {
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`
	Region   string `json:"region"`
}

func (c *IEXCloud) NewExchangeService() divyield.ExchangeService {
	return &exchangeService{
		IEXCloud: c,
	}
}

type exchangeService struct {
	*IEXCloud
}

func (s *exchangeService) Fetch(
	ctx context.Context,
	in *divyield.ExchangeFetchInput,
) (*divyield.ExchangeFetchOutput, error) {

	u := s.internationalExchangesURL()
	resp, err := s.httpGet(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	//fmt.Printf("%v: %v %v\n", in.ISIN, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	exchanges, err := s.parseInternationalExchanges(resp.Body)
	if err != nil {
		return nil, fmt.Errorf(
            "parse international exchanges: %s", 
            err,
        )
	}

	out := &divyield.ExchangeFetchOutput{
		Exchanges: make([]*divyield.Exchange, 0, len(exchanges)),
	}

	for _, v := range exchanges {
		currency := regionCurrencyMap[v.Region]

		ex := &divyield.Exchange{
			Region:      v.Region,
			Exchange:    v.Exchange,
			Suffix:      v.ExchangeSuffix,
			Currency:    currency,
			Description: v.Description,
		}
		out.Exchanges = append(out.Exchanges, ex)
	}

	return out, nil
}

func (c *IEXCloud) parseInternationalExchanges(
	r io.Reader,
) ([]*exchange, error) {
	exchanges := make([]*exchange, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var v exchange
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf(
                "decode international exchange: %s", 
                err,
            )
		}

		exchanges = append(exchanges, &v)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	sortInternationalExchanges(exchanges)
	return exchanges, nil
}

func sortInternationalExchanges(a []*exchange) {
	sort.SliceStable(a, func(i, j int) bool {
		switch strings.Compare(a[i].Region, a[j].Region) {
		case -1:
			return true
		case 1:
			return false
		}

		switch strings.Compare(a[i].Exchange, a[j].Exchange) {
		case -1:
			return true
		case 1:
			return false
		}

		return a[i].ExchangeSuffix < a[j].ExchangeSuffix
	})
}

type exchange struct {
	Region         string `json:"region"`
	Exchange       string `json:"exchange"`
	ExchangeSuffix string `json:"exchangeSuffix"`
	Description    string `json:"description"`
}

var regionCurrencyMap = map[string]string{
	"BE": "EUR",
	"CA": "CAD",
	"CH": "CHF",
	"DE": "EUR",
	"DK": "DKK",
	"ES": "EUR",
	"FR": "EUR",
	"GB": "GBP",
	"HU": "HUF",
	"IT": "EUR",
	"JP": "JPY",
	"LU": "EUR",
	"NL": "EUR",
	"US": "USD",
}
type options struct {
	baseURL string
	token   string
	rateLimiter       *rate.Limiter
	timeout           time.Duration

	outputDir         string
	startDate         time.Time
	endDate           time.Time
	iexCloudAPITokens map[string]string
	force             bool
	logger            logger.Logger
	workers           int
	db                divyield.DB
}

type Option func(o options) options

func BaseURL(v string) Option {
	return func(o options) options {
		o.baseURL = v
		return o
	}
}

func Token(v string) Option {
	return func(o options) options {
		o.token = v
		return o
	}
}

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

func RateLimiter(l *rate.Limiter) Option {
	return func(o options) options {
		o.rateLimiter = l
		return o
	}
}

func DB(db divyield.DB) Option {
	return func(o options) options {
		o.db = db
		return o
	}
}

func Timeout(d time.Duration) Option {
	return func(o options) options {
		o.timeout = d
		return o
	}
}

func IEXCloudAPITokens(t map[string]string) Option {
	return func(o options) options {
		o.iexCloudAPITokens = t
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
	outputDir: "",
	//startDate:   time.Date(2021, time.April, 23, 0, 0, 0, 0, time.UTC),
	startDate:   time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC),
	endDate:     time.Time{},
	workers:     1,
	rateLimiter: rate.NewLimiter(rate.Every(1*time.Second), 1),
	timeout:     0,
	logger:      nil,
}

func NewStockFetcher(os ...Option) *StockFetcher {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	return &StockFetcher{
		client: &httprate.RLClient{
			Client: &http.Client{
				Timeout: opts.timeout,
			},
			Ratelimiter: opts.rateLimiter,
		},
		opts: opts,
	}
}

type StockFetcher struct {
	db *divyield.DB

	client *httprate.RLClient
	opts   options
	errs   []error
}

func (f *StockFetcher) Fetch(ctx context.Context, tickers []string) {
	if f.opts.endDate.IsZero() {
		f.opts.endDate = time.Now()
	}

	err := f.opts.db.InitSchema(ctx, tickers)
	if err != nil {
		e := fmt.Errorf("init schema: %v", err)
		f.errs = append(f.errs, e)
		return
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
			//for job := range jobCh {
			//err := f.getStockData(ctx, job.Ticker)
			//resultCh <- result{Ticker: job.Ticker, Err: err}
			//}
		}()
	}

	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for res := range resultCh {
			if res.Err != nil {
				e := &FetchError{Ticker: res.Ticker, Err: res.Err}
				f.errs = append(f.errs, e)
				f.log("%v", e)
			} else {
				f.log("%v: %v", res.Ticker, "OK")
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

		//time.Sleep(time.Second * 1)
		pendingWg.Add(1)
		jobCh <- job{Ticker: ticker}
	}

	pendingWg.Wait()
	close(resultCh)
	resultWg.Wait()

	close(jobCh)
	workerWg.Wait()
}

func (f *StockFetcher) log(format string, v ...interface{}) {
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

func timeDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func timeRange(d1, d2 time.Time) string {
	if d1.IsZero() {
		return "5y"
	}

	if d1.After(d2) {
		panic(fmt.Sprintf("%v is greater then %v", d1, d2))
	}

	days := d2.Sub(d1).Hours() / 24

	if days < 28 {
		return "1m"
	}
	if days < 86 {
		return "3m"
	}
	if days < 178 {
		return "6m"
	}
	if days < 365 {
		return "1y"
	}
	if days < 730 {
		return "2y"
	}
	if days < 1825 {
		return "5y"
	}

	panic(fmt.Sprintf("too long time range: %v -> %v", d1, d2))
}

type date time.Time

const DateFormat = "2006-01-02"

func (t date) IsZero() bool {
	return time.Time(t).IsZero()
}

func (t date) After(o time.Time) bool {
	return time.Time(t).After(o)
}

func (t date) UntilDays(p time.Time) int64 {
	st := time.Time(t)
	if st.IsZero() {
		return -1
	}
	return int64(p.Sub(st) / (24 * time.Hour))
}

func (t date) Equal(o date) bool {
	return time.Time(t).Equal(time.Time(o))
}

func (t date) String() string {
	st := time.Time(t)
	if st.IsZero() {
		return "0000-00-00"
	}
	return st.Format(DateFormat)
}

func (t date) MarshalJSON() ([]byte, error) {
	st := time.Time(t)
	if st.IsZero() {
		return []byte("\"0000-00-00\""), nil
	}
	s := fmt.Sprintf("%q", st.Format(DateFormat))
	return []byte(s), nil
}

func (t *date) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "0000-00-00" {
		*t = date(time.Time{})
		return nil
	}

	st, err := time.Parse(DateFormat, s)
	if err != nil {
		return err
	}
	*t = date(st)
	return nil
}

type timeUnix time.Time

func (t timeUnix) Equal(o timeUnix) bool {
	return time.Time(t).Equal(time.Time(o))
}

func (t timeUnix) String() string {
	st := time.Time(t)
	if st.IsZero() {
		return "0"
	}
	return strconv.FormatInt(time.Time(t).Unix()*1000, 10)
}

func (t timeUnix) MarshalJSON() ([]byte, error) {
	st := time.Time(t)
	if st.IsZero() {
		return []byte("0"), nil
	}
	return json.Marshal(time.Time(t).Unix() * 1000)
}

func (t *timeUnix) UnmarshalJSON(b []byte) error {
	var i int64
	if err := json.Unmarshal(b, &i); err != nil {
		return err
	}
	*t = timeUnix(time.Unix(i/1000, 0))
	return nil
}

func (f *StockFetcher) Errs() []error {
	return f.errs
}

type FetchError struct {
	Ticker string
	Err    error
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("%v: %v", e.Ticker, e.Err)
}
