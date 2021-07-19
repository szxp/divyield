package yahoo

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
    "regexp"

	//"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"szakszon.com/divyield"
)

type options struct {
	timeout time.Duration
}

type Option func(o options) options

func Timeout(d time.Duration) Option {
	return func(o options) options {
		o.timeout = d
		return o
	}
}

var defaultOptions = options{
	timeout: 0,
}

func NewFinancialsService(
	os ...Option,
) divyield.FinancialsService {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	return &financialsService{
		opts: opts,
	}
}

type financialsService struct {
	opts options
}

func (s *financialsService) CashFlow(
	ctx context.Context,
	in *divyield.FinancialsCashFlowInput,
) (*divyield.FinancialsCashFlowOutput, error) {
	fcf, err := s.cashFlow(in.Symbol)
	if err != nil {
		return nil, err
	}

	periods := fcf[0]
	divsPaid := fcf[1]
	fcfs := fcf[2]

	cfs := make([]*divyield.FinancialsCashFlow, 0, len(periods))
	for i, period := range periods {
		divPaid := ""
		if i < len(divsPaid) {
			divPaid = divsPaid[i]
		}
		fcf := ""
		if i < len(fcfs) {
			fcf = fcfs[i]
		}
		cf, err := s.parseCashFlow(period, divPaid, fcf)
		if err != nil {
			return nil, err
		}
		cfs = append(cfs, cf)
	}

	return &divyield.FinancialsCashFlowOutput{
		CashFlow: cfs,
	}, nil
}

func (s *financialsService) cashFlow(
	symbol string,
) ([][]string, error) {
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

	u := "https://finance.yahoo.com/quote/" +
		symbol +
		"/cash-flow"

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
			"//span[contains(text(),'Collapse All')]",
			chromedp.BySearch,
		),
		chromedp.Evaluate(extractCashFlowJS, &res),
	)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *financialsService) parseCashFlow(
	period string,
	divPaidStr string,
	fcfStr string,
) (*divyield.FinancialsCashFlow, error) {
	var divPaid float64
	var fcf float64
	var err error

	if divPaidStr != "" && divPaidStr != "-" {
		divPaidStr = strings.ReplaceAll(divPaidStr, ",", "")
		divPaid, err = strconv.ParseFloat(divPaidStr, 64)
		if err != nil {
			return nil, err
		}
	}

	if fcfStr != "" && fcfStr != "-" {
		fcfStr = strings.ReplaceAll(fcfStr, ",", "")
		fcf, err = strconv.ParseFloat(fcfStr, 64)
		if err != nil {
			return nil, err
		}
	}

	return &divyield.FinancialsCashFlow{
		Period:       period,
		DividendPaid: divPaid,
		FreeCashFlow: fcf,
	}, nil
}

func (s *financialsService) PullValuation(
	ctx context.Context,
	in *divyield.FinancialsPullValuationInput,
) (
    chan string,
    chan *divyield.FinancialsPullValuationOutput,
) {
	resCh := make(
        chan *divyield.FinancialsPullValuationOutput,
    )
	jobCh := make(chan string)

	go func() {
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

		responses := make(map[string]*response)
		statementsCh := make(chan *response)

		chromedp.ListenTarget(ctx, func(v interface{}) {
			switch ev := v.(type) {
			case *network.EventRequestWillBeSent:
                reqID := ev.RequestID.String()
				resp := &response{
					URL: ev.Request.URL,
				}

				if (resp.IsIS() || resp.IsBS() || resp.IsCF() || resp.IsValuation()) &&
                    ev.Request.Method == "GET" {
				    //fmt.Println("reqID", reqID)
					responses[reqID] = resp
				}

			case *network.EventLoadingFinished:
				for reqID, resp := range responses {
					if reqID == ev.RequestID.String() {
						//fmt.Println("del reqID", reqID)
						delete(responses, reqID)
						go func() {
							c := chromedp.FromContext(ctx)
							rbp := network.GetResponseBody(
								ev.RequestID,
							)
							body, err := rbp.Do(
								cdp.WithExecutor(
									ctx,
									c.Target,
								),
							)
							if err != nil {
								fmt.Println(err)
							} else {
								resp.Body = string(body)
								statementsCh <- resp
							}
						}()
						break
					}
				}
			}
		})

		for u := range jobCh {
			var compID string
			actions := make([]chromedp.Action, 0)
			actions = append(
				actions,
				chromedp.Navigate(u + "/valuation"),
				chromedp.Evaluate(libJS, &[]byte{}),
				chromedp.Evaluate(extractCompID, &compID),
				runWithTimeOut(&ctx, 5, chromedp.Tasks{
					chromedp.WaitVisible(
						"//span[contains(text(),'Price/Earnings')]",
						chromedp.BySearch,
					),
				}),
				chromedp.Navigate(u + "/financials"),
				runWithTimeOut(&ctx, 5, chromedp.Tasks{
					chromedp.WaitVisible(
						"//span[contains(text(),'Normalized Diluted EPS')]",
						chromedp.BySearch,
					),
				}),

		        chromedp.Evaluate(libJS, &[]byte{}),
				chromedp.Evaluate(clickDetailsViewLink, &[]byte{}),
				chromedp.Sleep(2 * time.Second),
                /*
				runWithTimeOut(&ctx, 5, chromedp.Tasks{
					chromedp.WaitVisible(
						"//div[contains(text(),'Total Revenue')]",
						chromedp.BySearch,
					),
				}),
                */

				chromedp.Evaluate(clickBalSheRadio, &[]byte{}),
				chromedp.Sleep(2 * time.Second),
                /*
				runWithTimeOut(&ctx, 5, chromedp.Tasks{
					chromedp.WaitVisible(
						"//div[contains(text(),'Total Assets')]",
						chromedp.BySearch,
					),
				}),
                */

				chromedp.Evaluate(clickCasFloRadio, &[]byte{}),
				chromedp.Sleep(2 * time.Second),
                /*
				runWithTimeOut(&ctx, 5, chromedp.Tasks{
					chromedp.WaitVisible(
						"//div[contains(text(),'Cash Flow from Operating Activities')]",
						chromedp.BySearch,
					),
				}),
                */
			)

			res := &divyield.FinancialsPullValuationOutput{
				URL: u,
			}

			err := chromedp.Run(ctx, actions...)
			if err != nil {
				res.Err = err
				resCh <- res
				continue
			}

            if compID == "" {
				res.Err = fmt.Errorf("CompID not found")
				resCh <- res
				continue
            }
			//fmt.Println("compID", compID)

            var resp *response
			var is, bs, cf, val string
			for {
				select {
                case resp = <-statementsCh:
				case <-time.After(10 * time.Second):
					res.Err = fmt.Errorf("Response timeout")
					resCh <- res
					continue
				}

				if resp.CompID() != compID {
					fmt.Printf(
						"Ignore unexpected compID: %v\n",
						resp.CompID(),
					)
					continue
                }

				if resp.IsIS() {
					is = resp.Body
				} else if resp.IsBS() {
					bs = resp.Body
				} else if resp.IsCF() {
					cf = resp.Body
				} else if resp.IsValuation() {
					val = resp.Body
				} else {
					res.Err = fmt.Errorf(
						"Unexpected statement: %v",
						resp.Body,
					)
					resCh <- res
					continue
				}

				if is != "" &&
                    bs != "" &&
                    cf != "" &&
                    val != "" {
					break
				}
			}

			res.Valuation = val
			res.IncomeStatement = is
			res.BalanceSheet = bs
			res.CashFlow = cf
			resCh <- res
		}
		close(resCh)
	}()

	return jobCh, resCh
}

type response struct {
	URL  string
	Body string
}

func (r *response) CompID() string {
    if r.IsValuation() {
	    re := regexp.MustCompile(`/valuation/[^/]+/([^/]+)\?`)
	    matches := re.FindStringSubmatch(r.URL)
	    return matches[1]
    }

	re := regexp.MustCompile(`/newfinancials/([^/]+)/`)
	matches := re.FindStringSubmatch(r.URL)
	return matches[1]
}

func (r *response) IsIS() bool {
	return strings.Contains(r.URL, "incomeStatement/detail")
}

func (r *response) IsBS() bool {
	return strings.Contains(r.URL, "balanceSheet/detail")
}

func (r *response) IsCF() bool {
	return strings.Contains(r.URL, "cashFlow/detail")
}

func (r *response) IsValuation() bool {
	return strings.Contains(r.URL, "/valuation/")
}

func changeTail(
	u string,
	tail string,
) string {
	parts := strings.Split(u, "/")
	parts = parts[0 : len(parts)-1]
	parts = append(parts, tail)
	return strings.Join(parts, "/")
}

func runWithTimeOut(
	ctx *context.Context,
	timeout time.Duration,
	tasks chromedp.Tasks,
) chromedp.ActionFunc {
	return func(ctx context.Context) error {
		timeoutContext, cancel := context.WithTimeout(
			ctx,
			timeout*time.Second,
		)
		defer cancel()
		return tasks.Do(timeoutContext)
	}
}

const libJS = `
function compID() {
    var scripts = document.querySelectorAll('script');
    var i;
    var match;
    var s;
    var re = /byId:{"([^"]+)"/;
    for (i=0; i<scripts.length; i++) {
        s = scripts[i].innerText;
        if (s.includes('byId:{')) {
            match = s.match(re);
            console.log(match);
            return match[1];
        }
    }
    return "";
}


function valuation() {
    var i, k;
    var tr;
    var trs = [];
    var cells;
    var cell;
    var res = [];
    var row;
    var thread;

    thead = document.querySelector('.report-table .thead');
    trs.push(thead);
    rows = document.querySelectorAll('.report-table .report-table-row');
    for (i=0; i<rows.length; i++) {
        trs.push(rows[i]);
    }

    for (i=0; i<trs.length; i++) {
        tr = trs[i];
        if (tr.classList.contains('chart-row')) {
            continue;
        }

        cells = tr.querySelectorAll('td');
        row = [];
        for (k=0; k<cells.length; k++) {
            cell = cells[k];
            row.push(cell.innerText.trim());
        }
        res.push(row);
    }
    return res;
}


function exportExcel() {
  document.querySelector('.sal-financials-details__export').click();
}

function clickA(label) {
  var i;
  var els = document.querySelectorAll('a');
  for (i=0; i < els.length; i++) {
    if (els[i].innerText.includes(label)) {
      els[i].click();
    }
  }
}

function clickRadio(label) {
  var i;
  var els = document.querySelectorAll('input[type="radio"]');
  for (i=0; i < els.length; i++) {
    if (els[i].getAttribute('value').includes(label)) {
      els[i].click();
    }
  }
}
`
const extractCompID = `compID();`

const extractValuation = `valuation();`

const clickIncStatLink = `clickA('Income Statement');`

const clickDetailsViewLink = `clickA('Details View');`

const clickIncStaRadio = `clickRadio('Income Statement');`

const clickBalSheRadio = `clickRadio('Balance Sheet');`

const clickCasFloRadio = `clickRadio('Cash Flow');`

const clickExport = `exportExcel();`

const clickExpandBtnJS = `
function cellsContent(root) {
    var label, cells, c, i, res = [];
    if (!root) {
        return res;
    }

    label = root.getElementsByClassName('D(ib)')[0].textContent.trim();
    res = res.concat([label]);

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



function rows(root) {
    var lines = [];
    var i;
    var rows = root.getElementsByClassName('D(tbr)')
    for (i=0; i<rows.length; i++) {
        lines = lines.concat([cellsContent(rows[i])]);
    }
    return lines;
}


function prevClose(root) {
    return parseFloat(root.querySelector('td[data-test="PREV_CLOSE-value"]').textContent);
}


function symbol(root) {
    return root.querySelector('h1').textContent.match(/.*\((.+)\)/)[1]
}

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

const funcsJS = `

`

const extractCashFlowJS = `
parse(document);
`

const extractBalanceSheetsJS = `
rows(document);
`

const extractPrevCloseJS = `
prevClose(document);
`

const extractSymbolJS = `
symbol(document);
`

const extractPageJS = `
function page() {
    return "page";
    return document.getElementsByTagName("html")[0].innerHTML;
}
page();
`
