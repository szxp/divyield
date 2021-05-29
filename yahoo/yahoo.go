package yahoo

import (
	"context"
	"log"
	"time"
    "strconv"
    "strings"

	"github.com/chromedp/chromedp"
	"szakszon.com/divyield"
)

type options struct {
	timeout     time.Duration
}

type Option func(o options) options

func Timeout(d time.Duration) Option {
	return func(o options) options {
		o.timeout = d
		return o
	}
}


var defaultOptions = options{
	timeout:     0,
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
	opts   options
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
        cf, err :=  s.parse(period, divPaid, fcf)
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

	u := "https://finance.yahoo.com/quote/"+
    symbol+
    "/cash-flow?p=CVX"

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
		chromedp.Evaluate(extractJS, &res),
	)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *financialsService) parse(
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
        Period: period,
        DividendPaid: divPaid,
        FreeCashFlow: fcf,
    }, nil
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
