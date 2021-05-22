package xrates

// https://www.x-rates.com/historical/?from=CAD&amount=1&date=2011-05-03

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"net/http"
//	"net/http/httputil"
	"regexp"
	"strconv"
	"strings"
	"time"

	"szakszon.com/divyield"
	"szakszon.com/divyield/httprate"
	"szakszon.com/divyield/logger"
)

type currencyService struct {
	client *httprate.RLClient
	opts   options
}

func NewCurrencyService(os ...Option) divyield.CurrencyService {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	stdClient := &http.Client{
		Timeout: opts.timeout,
	}

	rlClient := &httprate.RLClient{
		Client:      stdClient,
		Ratelimiter: opts.rateLimiter,
	}

	return &currencyService{
		client: rlClient,
		opts:   opts,
	}
}

func (cc *currencyService) Convert(
	ctx context.Context,
	in *divyield.CurrencyConvertInput,
) (*divyield.CurrencyConvertOutput, error) {

	u := cc.ratesURL(in.From, 1, in.Date)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", cc.opts.userAgent)

//	dump, err := httputil.DumpRequestOut(req, true)
//	if err != nil {
//		return nil, err
//	}
//	cc.logf("%s", dump)

	resp, err := cc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	cc.logf("%v %v", resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	rate, err := cc.parseRate(in.From, in.To, string(body))
	if err != nil {
		return nil, err
	}

	out := &divyield.CurrencyConvertOutput{
		Amount: in.Amount * rate,
		Rate:   rate,
	}
	return out, nil
}

func (cc *currencyService) ratesURL(
	from string,
	amount float64,
	date time.Time,
) string {
	return "https://www.x-rates.com/historical/" +
		"?from=" + strings.ToUpper(from) +
		"&amount=" + strconv.FormatFloat(amount, 'f', -1, 64) +
		"&date=" + date.Format(divyield.DateFormat)
}

func (cc *currencyService) parseRate(
	from string,
	to string,
	s string,
) (float64, error) {
	// Example URL:
	//<a href='https://www.x-rates.com/graph/?from=CAD&amp;to=USD'>0.829220</a>
	re := regexp.MustCompile(
		`<a[^>]+from=` + from + `&amp;to=` + to + `[^>]+>([0-9\.]+)</a>`)
	matches := re.FindStringSubmatch(s)
	//cc.logf("%v", matches)

	if len(matches) < 2 {
		return 0, fmt.Errorf("no rate")
	}

	return strconv.ParseFloat(matches[1], 64)
}

func (cc *currencyService) logf(
	format string,
	v ...interface{},
) {
	w := cc.opts.logger
	if w != nil {
		w.Logf(format, v...)
	}
}

const defaultUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36 OPR/76.0.4017.123"

var defaultOptions = options{
	rateLimiter: rate.NewLimiter(rate.Every(1*time.Second), 1),
	userAgent:   defaultUA,
	timeout:     0,
	logger:      nil,
}

type options struct {
	rateLimiter *rate.Limiter
	userAgent   string
	timeout     time.Duration
	logger      logger.Logger
}

type Option func(o options) options

func RateLimiter(l *rate.Limiter) Option {
	return func(o options) options {
		o.rateLimiter = l
		return o
	}
}

func Timeout(d time.Duration) Option {
	return func(o options) options {
		o.timeout = d
		return o
	}
}

func Logger(v logger.Logger) Option {
	return func(o options) options {
		o.logger = v
		return o
	}
}
