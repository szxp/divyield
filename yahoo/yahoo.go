package yahoo

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

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

func (s *financialsService) BalanceSheets(
	ctx context.Context,
	in *divyield.FinancialsBalanceSheetsInput,
) (*divyield.FinancialsBalanceSheetsOutput, error) {
	rows, symbolYahoo, err := s.balanceSheets(
		in.Symbol,
		in.PeriodLength,
	)
	if err != nil {
		return nil, err
	}

	sheets := make([]*divyield.FinancialsBalanceSheet, 0)
	for i := 1; i <= len(rows[0])-1; i++ {
		sheet, err := s.parseBalanceSheet(rows, i)
		if err != nil {
			return nil, err
		}
		sheets = append(sheets, sheet)
	}

	return &divyield.FinancialsBalanceSheetsOutput{
		Symbol:        symbolYahoo,
		BalanceSheets: sheets,
	}, nil
}

func (s *financialsService) balanceSheets(
	symbol string,
	periodLength string,
) ([][]string, string, error) {
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

	byISIN := 12 <= len(symbol)

	var u string
	if byISIN {
		u = "https://finance.yahoo.com/quote/" +
			"?yfin-usr-qry=" + symbol
	} else {
		u = "https://finance.yahoo.com/quote/" +
			symbol + "/balance-sheet"
	}

	var res [][]string
	var symbolYahoo string

	actions := make([]chromedp.Action, 0)
	actions = append(
		actions,
		chromedp.Navigate(u),
		runWithTimeOut(&ctx, 10, chromedp.Tasks{
			chromedp.WaitVisible(
				"form.consent-form button",
				chromedp.ByQuery,
			),
			chromedp.Click(
				"form.consent-form button",
				chromedp.ByQuery,
			),
		}),
	)

	if byISIN {
		actions = append(
			actions,
			runWithTimeOut(&ctx, 10, chromedp.Tasks{
				chromedp.WaitVisible(
					"//a/span[contains(text(),'Financials')]",
					chromedp.BySearch,
				),
				chromedp.Click(
					"//a/span[contains(text(),'Financials')]",
					chromedp.BySearch,
				),
			}),
			runWithTimeOut(&ctx, 10, chromedp.Tasks{
				chromedp.WaitVisible(
					"//div/span[contains(text(),'Balance Sheet')]",
					chromedp.BySearch,
				),
				chromedp.Click(
					"//div/span[contains(text(),'Balance Sheet')]",
					chromedp.BySearch,
				),
				chromedp.WaitVisible(
					"//span[contains(text(),'Total Assets')]",
					chromedp.BySearch,
				),
			}),
		)
	}

	actions = append(
		actions,
		runWithTimeOut(&ctx, 10, chromedp.Tasks{
			chromedp.WaitVisible(
				"button.expandPf",
				chromedp.ByQuery,
			),
		}),
		chromedp.Evaluate(clickExpandBtnJS, &[]byte{}),
		chromedp.WaitVisible(
			"//span[contains(text(),'Collapse All')]",
			chromedp.BySearch,
		),
	)

	//if periodLength == "Quarterly" {
	actions = append(
		actions,
		chromedp.WaitVisible(
			"//button/div/span[contains(text(),'Quarterly')]",
			chromedp.BySearch,
		),
		chromedp.Click(
			"//button/div/span[contains(text(),'Quarterly')]",
			chromedp.BySearch,
		),
		chromedp.WaitVisible(
			"//button/div/span[contains(text(),'Annual')]",
			chromedp.BySearch,
		),
	)
	//}

	actions = append(
		actions,
		chromedp.Evaluate(extractBalanceSheetsJS, &res),
		chromedp.Evaluate(extractSymbolJS, &symbolYahoo),
	)

	err := chromedp.Run(ctx, actions...)
	if err != nil {
		return nil, "", err
	}

	return res, symbolYahoo, nil
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

func (s *financialsService) parseBalanceSheet(
	rows [][]string,
	i int,
) (*divyield.FinancialsBalanceSheet, error) {
	sheet := &divyield.FinancialsBalanceSheet{
		Entries: make([]*divyield.FinancialsBalanceSheetEntry, 0),
	}

	for _, row := range rows {
		if row[0] == "Breakdown" {
			period, err := time.Parse("1/2/2006", row[i])
			if err != nil {
				return nil, err
			}
			sheet.Period = period
		} else {
			v, err := parseNumber(row[i])
			if err != nil {
				return nil, err
			}

			e := &divyield.FinancialsBalanceSheetEntry{
				Key:   strings.ReplaceAll(row[0], ",", ""),
				Value: v,
			}
			sheet.Entries = append(sheet.Entries, e)
		}
	}
	return sheet, nil
}

func parseNumber(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0.0, nil
	}

	s = strings.ReplaceAll(s, ",", "")
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0, err
	}

	v = v * 1000.0 // Numbers in thousands on Yahoo
	return v, nil
}

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
