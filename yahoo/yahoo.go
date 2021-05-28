package yahoo

import (
	"context"
	"encoding/csv"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	"szakszon.com/divyield"
	"szakszon.com/divyield/httprate"
	"szakszon.com/divyield/logger"
)

type options struct {
	rateLimiter *rate.Limiter
	timeout     time.Duration // http client timeout, 0 means no timeout
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

func Log(l logger.Logger) Option {
	return func(o options) options {
		o.logger = l
		return o
	}
}

var defaultOptions = options{
	rateLimiter: rate.NewLimiter(rate.Every(1*time.Second), 1),
	timeout:     0,
	logger:      nil,
}

// SplitFetcher is safe for concurrent use by multiple goroutines.
func NewSplitFetcher(os ...Option) *SplitFetcher {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	return &SplitFetcher{
		client: &httprate.RLClient{
			Client: &http.Client{
				Timeout: opts.timeout,
			},
			Ratelimiter: opts.rateLimiter,
		},
		opts: opts,
	}
}

// SplitFetcher is safe for concurrent use by multiple goroutines.
type SplitFetcher struct {
	client *httprate.RLClient
	opts   options
}

func (f *SplitFetcher) Fetch(
	ctx context.Context,
	ticker string,
	startDate time.Time,
	endDate time.Time,
) ([]*divyield.Split, error) {
	if startDate.IsZero() {
		startDate = time.Date(1800, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	if endDate.IsZero() {
		endDate = time.Now().UTC()
	}

	if ticker == "BF.B" {
		ticker = "BF-B"
	}

	u := splitsURL(ticker, startDate, endDate)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	f.log("%v: %v %v", ticker, resp.StatusCode, u)

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf("http error: %d", resp.StatusCode)
	}

	splits, err := parseSplits(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse splits: %s", err)
	}

	sortSplitsDesc(splits)

	return splits, nil
}

func splitsURL(
	ticker string,
	startDate time.Time,
	endDate time.Time,
) string {
	return "https://query1.finance.yahoo.com" +
		"/v7/finance/download/" + strings.ToUpper(ticker) +
		"?period1=" + strconv.FormatInt(startDate.Unix(), 10) +
		"&period2=" + strconv.FormatInt(endDate.Unix(), 10) +
		"&interval=1d" +
		"&events=split" +
		"&includeAdjustedClose=true"
}

func parseSplits(in io.Reader) ([]*divyield.Split, error) {
	splits := make([]*divyield.Split, 0)

	records := make([][]string, 0)
	r := csv.NewReader(in)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	if len(records) > 0 {
		// forget the header row
		records = records[1:]

		for _, rec := range records {
			exDate, err := time.Parse(divyield.DateFormat, rec[0])
			if err != nil {
				return nil, err
			}

			factors := strings.Split(rec[1], ":")
			toFactor, err := strconv.Atoi(factors[0])
			if err != nil {
				return nil, err
			}
			fromFactor, err := strconv.Atoi(factors[1])
			if err != nil {
				return nil, err
			}

			split := &divyield.Split{
				ExDate:     exDate,
				ToFactor:   toFactor,
				FromFactor: fromFactor,
			}

			splits = append(splits, split)
		}
	}

	return splits, nil
}

func sortSplitsDesc(a []*divyield.Split) {
	sort.SliceStable(a, func(i, j int) bool {
		return a[i].ExDate.After(a[j].ExDate)
	})
}

func (f *SplitFetcher) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}

func fcf() {
	opts := append(
		chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
	)
	actx, cancel := chromedp.NewExecAllocator(
		context.Background(),
		opts...,
	)
	ctx, cancel := chromedp.NewContext(
		actx,
		chromedp.WithLogf(log.Printf),
		//chromedp.WithDebugf(log.Printf),
		chromedp.WithErrorf(log.Printf),
	)
	defer cancel()

	u := "https://finance.yahoo.com/quote/CVX/cash-flow?p=CVX"

	var res [][]string
	err := chromedp.Run(ctx,
		chromedp.Navigate(u),
		chromedp.Click(
			"form.consent-form button",
			chromedp.ByQuery,
			chromedp.NodeVisible,
		),
		chromedp.WaitVisible(
			"button.expandPf",
			chromedp.ByQuery,
		),
		chromedp.Evaluate(clickExpandBtnJS, &[]byte{}),
		chromedp.WaitVisible(
			"button[aria-label=\"Cash Dividends Paid\"]",
			chromedp.ByQuery,
		),
		chromedp.Evaluate(extractJS, &res),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(res)
}

const clickExpandBtnJS = `
var clickExpandBtn = async function(root) {
    var t = setInterval(function(){
        var btn = root.querySelector('button.expandPf');  
        if (btn) {
            if (btn.textContent.includes('Collapse All')) {
                clearInterval(t);
            } else {
                btn.click();
            }
        }
    }, 1000);
}

clickExpandBtn(document);
`

const extractJS = `
function cellsContent(root) {
    var cells, c, i, res = [];
    if (!root) {
        return res;
    }
    cells = root.getElementsByClassName('Ta(c)');
    for (i=0; i<cells.length; i++) {
        c = cells[i];
        res = res.concat([c.textContent.trim()]);
    }
    return res;
}

function findRow(root, label) {
    var i;
    var rows = root.getElementsByClassName('D(tbr)')
    for (i=0; i<rows.length; i++) {
        if (rows[i].textContent.includes(label)) {
            return rows[i];
        }
    }
}

function parse(root) {
    var lines = [];
    lines = lines.concat([cellsContent(findRow(root, 'Breakdown'))]);
    lines = lines.concat([cellsContent(findRow(root, 'Cash Dividends Paid'))]);
    lines = lines.concat([cellsContent(findRow(root, 'Free Cash Flow'))]);
    return lines;
}

parse(document);

`
