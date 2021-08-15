package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"text/template"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"szakszon.com/divyield"
)

type Command struct {
	name string
	opts options
	args []string
}

func NewCommand(
	name string,
	args []string,
	os ...Option,
) *Command {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}

	return &Command{
		name: name,
		opts: opts,
		args: args,
	}
}

func (c *Command) Execute(ctx context.Context) error {
	switch c.name {
	case "pull":
		return c.pull(ctx)
	case "stats":
		return c.stats(ctx)
	case "bargain":
		return c.bargain(ctx)
	case "pull-valuation":
		return c.pullValuation(ctx)
	case "profile":
		return c.profile(ctx)
	case "symbols":
		return c.symbols(ctx)
	case "exchanges":
		return c.exchanges(ctx)
	default:
		return fmt.Errorf("invalid command: %v", c.name)
	}
}

func (c *Command) stats(ctx context.Context) error {
	var err error

	symbols, err := c.resolveSymbols(ctx, c.args)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("Symbol not found")
	}

	infout, err := c.opts.inflationService.Fetch(
		ctx,
		&divyield.InflationFetchInput{},
	)
	if err != nil {
		return err
	}

	spout, err := c.opts.sp500Service.DividendYield(
		ctx,
		&divyield.SP500DividendYieldInput{},
	)
	if err != nil {
		return err
	}

	sg := &statsGenerator{
		db:                  c.opts.db,
		startDate:           c.opts.startDate,
		inflation:           &infout.Inflation,
		sp500DividendYield:  &spout.SP500DividendYield,
		divYieldFwdSP500Min: c.opts.divYieldFwdSP500Min,
		divYieldFwdSP500Max: c.opts.divYieldFwdSP500Max,
		divYieldTotalMin:    c.opts.divYieldTotalMin,
		ggrROI:              c.opts.ggrROI,
		ggrMin:              c.opts.ggrMin,
		ggrMax:              c.opts.ggrMax,
		noCutDividend:       c.opts.noCutDividend,
		noDecliningDGR:      c.opts.noDecliningDGR,
		dgrAvgMin:           c.opts.dgrAvgMin,
		dgrYearly:           c.opts.dgrYearly,
	}

	stats, err := sg.Generate(ctx, symbols)
	if err != nil {
		return err
	}

	if c.opts.chart {
		cg := &chartGenerator{
			db:        c.opts.db,
			writer:    c.opts.writer,
			dir:       c.opts.dir,
			startDate: c.opts.startDate,
		}
		err = cg.Generate(ctx, stats)
		if err != nil {
			return err
		}
	}

	c.writeStats(stats)
	c.writeStatsFooter(sg, stats)
	return nil
}

func (c *Command) writeStats(s *divyield.Stats) {
	out := &bytes.Buffer{}
	w := tabwriter.NewWriter(
		out, 0, 0, 2, ' ', tabwriter.AlignRight)

	b := &bytes.Buffer{}
	b.WriteString(fmt.Sprintf("%-38v", "Company"))
	b.WriteByte('\t')
	b.WriteString(fmt.Sprintf("%-33v", "Exchange"))
	b.WriteByte('\t')
	b.WriteString("Dividend fwd")
	b.WriteByte('\t')
	b.WriteString("Yield fwd")
	b.WriteByte('\t')
	b.WriteString("GGR")
	b.WriteByte('\t')
	b.WriteString("MR% date")
	b.WriteByte('\t')
	b.WriteString("MR%")
	b.WriteByte('\t')
	b.WriteString("DGR-1y")
	b.WriteByte('\t')
	b.WriteString("DGR-2y")
	b.WriteByte('\t')
	b.WriteString("DGR-3y")
	b.WriteByte('\t')
	b.WriteString("DGR-4y")
	b.WriteByte('\t')
	//b.WriteString("DGR-5y")
	//b.WriteByte('\t')

	fmt.Fprintln(w, b.String())

	for _, row := range s.Rows {
		b.Reset()
		b.WriteString(fmt.Sprintf(
			"%-38v",
			row.Profile.Symbol+" - "+row.Profile.Name,
		))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf(
			"%-33v",
			row.Profile.Exchange,
		))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f", row.DivFwd))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DivYieldFwd))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.GordonGrowthRate))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf(
			"%s",
			row.DividendChangeMRDate.Format("2006-01-02")))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf(
			"%.2f%%",
			row.DividendChangeMR))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[1]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[2]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[3]))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[4]))
		//b.WriteByte('\t')
		//b.WriteString(fmt.Sprintf("%.2f%%", row.DGRs[5]))
		b.WriteByte('\t')

		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", out.String())
}

func (c *Command) writeStatsFooter(
	sg *statsGenerator,
	stats *divyield.Stats,
) {
	out := &bytes.Buffer{}
	w := tabwriter.NewWriter(
		out, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}

	b.Reset()
	b.WriteString("Number of companies:")
	b.WriteByte('\t')
	b.WriteString(strconv.Itoa(len(stats.Rows)))
	b.WriteByte('\t')
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Start date:")
	b.WriteByte('\t')
	b.WriteString(sg.startDate.Format(divyield.DateFormat))
	b.WriteByte('\t')
	fmt.Fprintln(w, b.String())

	inf := fmt.Sprintf(
		"%.2f%%, %v",
		sg.inflation.Rate,
		sg.inflation.Period,
	)

	b.Reset()
	b.WriteString("Inflation (HUN current):")
	b.WriteByte('\t')
	b.WriteString(inf)
	b.WriteByte('\t')
	fmt.Fprintln(w, b.String())

	sp500DivYld := fmt.Sprintf(
		"%.2f%%, %v",
		sg.sp500DividendYield.Rate,
		sg.sp500DividendYield.Timestamp,
	)

	b.Reset()
	b.WriteString("S&P 500 dividend yield:")
	b.WriteByte('\t')
	b.WriteString(sp500DivYld)
	b.WriteByte('\t')
	fmt.Fprintln(w, b.String())

	if sg.divYieldTotalMin > 0 {
		b.Reset()
		b.WriteString("Dividend yield total min:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", sg.divYieldTotalMin))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}
	if sg.divYieldFwdMin() > 0 {
		b.Reset()
		b.WriteString("Dividend yield fwd min:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf(
			"%.2f%%",
			sg.divYieldFwdMin(),
		))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}
	if sg.divYieldFwdMax() > 0 {
		b.Reset()
		b.WriteString("Dividend yield fwd max:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf(
			"%.2f%%",
			sg.divYieldFwdMax(),
		))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}

	if sg.ggrROI > 0 {
		b.Reset()
		b.WriteString("GGR ROI:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", sg.ggrROI))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}
	if sg.ggrMin > 0 {
		b.Reset()
		b.WriteString("GGR min:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", sg.ggrMin))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}
	if sg.ggrMax > 0 {
		b.Reset()
		b.WriteString("GGR max:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", sg.ggrMax))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}

	if sg.dgrAvgMin > 0 {
		b.Reset()
		b.WriteString("DGRAvg min:")
		b.WriteByte('\t')
		b.WriteString(fmt.Sprintf("%.2f%%", sg.dgrAvgMin))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}

	if sg.noCutDividend {
		b.Reset()
		b.WriteString("No cut dividend")
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}
	if sg.noDecliningDGR {
		b.Reset()
		b.WriteString("No declining DGR")
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}
	if sg.dgrYearly {
		b.Reset()
		b.WriteString("DGR yearly")
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", out.String())
}

func (c *Command) cashFlow(ctx context.Context) error {
	symbols, err := c.resolveSymbols(ctx, c.args)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("Symbol not found")
	}
	symbol := symbols[0]
	out, err := c.opts.financialsService.CashFlow(
		ctx,
		&divyield.FinancialsCashFlowInput{
			Symbol: symbol,
		},
	)
	if err != nil {
		return err
	}

	c.writeCashFlow(out.CashFlow)
	return nil
}

func (c *Command) writeCashFlow(
	f []*divyield.FinancialsCashFlow,
) {
	out := &bytes.Buffer{}
	w := tabwriter.NewWriter(
		out, 0, 0, 2, ' ', tabwriter.AlignRight)

	p := message.NewPrinter(language.English)

	b := &bytes.Buffer{}
	b.WriteString("Period")
	b.WriteByte('\t')
	b.WriteString("DPS/FCF")
	b.WriteByte('\t')
	b.WriteString("Dividend paid")
	b.WriteByte('\t')
	b.WriteString("Free cash flow")
	b.WriteByte('\t')
	fmt.Fprintln(w, b.String())

	for _, cf := range f {
		b.Reset()
		b.WriteString(cf.Period)
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f%%", cf.DPSPerFCF()))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", cf.DividendPaid))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", cf.FreeCashFlow))
		b.WriteByte('\t')
		fmt.Fprintln(w, b.String())
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "All numbers in thousands")

	w.Flush()
	c.writef("%s", out.String())
}

const (
	lastTTM = "TTM"
	last1   = "2020"
	last2   = "2019"
	last3   = "2018"
	last4   = "2017"
	last5   = "2016"
)

func (c *Command) bargain(ctx context.Context) error {
	baseDir := c.opts.dir
	if baseDir == "" {
		return fmt.Errorf("dir must be specified")
	}

	exist, err := exists(baseDir)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("dir not found: %v", baseDir)
	}

	urlsFile := c.args[0]
	uf, err := os.Open(urlsFile)
	if err != nil {
		return err
	}
	defer uf.Close()

	financials := make([]*financials, 0)
	scanner := bufio.NewScanner(uf)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			break
		default:
			// noop
		}

		u := scanner.Text()
		u = strings.TrimSpace(u)
		if u == "" || strings.HasPrefix(u, "#") {
			continue
		}

		_, symbol, exch := morningstarURLValuation(u)

		dir := filepath.Join(baseDir, exch, symbol)

		/*
					missingFile := filepath.Join(dir, "missing")
					exist, err = exists(missingFile)
					if err != nil {
						return err
					}
					if exist {
			            fmt.Println("missing")
						continue
					}
		*/

		fin, err := c.financials(ctx, dir)
		if err != nil {
			return fmt.Errorf("%v: %v", symbol, err)
		}
		if fin != nil {
			fin.Exchange = exch
			fin.Symbol = symbol

			//fin.PToFCFTTM = fin.PriceToFreeCashFlow(lastTTM)

//            fin.NetCashToMCap = fin.NetCashToMarketCap(last1)

			fin.E1 = fin.IncomeStatement.NetIncome(last1)
			fin.E2 = fin.IncomeStatement.NetIncome(last2)
			fin.E3 = fin.IncomeStatement.NetIncome(last3)
			fin.E4 = fin.IncomeStatement.NetIncome(last4)
			fin.E5 = fin.IncomeStatement.NetIncome(last5)

			fin.FCFPSTTM = fin.FreeCashFlowPerShare(last1)
			fin.FCFPS1 = fin.FreeCashFlowPerShare(last1)
			fin.FCFPS2 = fin.FreeCashFlowPerShare(last2)
			fin.FCFPS3 = fin.FreeCashFlowPerShare(last3)
			fin.FCFPS4 = fin.FreeCashFlowPerShare(last4)
			fin.FCFPS5 = fin.FreeCashFlowPerShare(last5)

			fin.BVPS1 = fin.BookValuePerShare(last1)
			fin.BVPS2 = fin.BookValuePerShare(last2)
			fin.BVPS3 = fin.BookValuePerShare(last3)
			fin.BVPS4 = fin.BookValuePerShare(last4)
			fin.BVPS5 = fin.BookValuePerShare(last5)

			fin.DivPS1 = fin.DividendPerShare(last1)
			fin.DivPS2 = fin.DividendPerShare(last2)
			fin.DivPS3 = fin.DividendPerShare(last3)
			fin.DivPS4 = fin.DividendPerShare(last4)
			fin.DivPS5 = fin.DividendPerShare(last5)

            fin.ROIC1 = fin.ReturnOnInvestedCapital(last1)
			fin.ROIC2 = fin.ReturnOnInvestedCapital(last2)
			fin.ROIC3 = fin.ReturnOnInvestedCapital(last3)
			fin.ROIC4 = fin.ReturnOnInvestedCapital(last4)
			fin.ROIC5 = fin.ReturnOnInvestedCapital(last5)

//			fin.ROE1 = fin.
//				ReturnOnEquity(last1)
//			fin.ROE2 = fin.
//				ReturnOnEquity(last2)
//			fin.ROE3 = fin.
//				ReturnOnEquity(last3)
//			fin.ROE4 = fin.
//				ReturnOnEquity(last4)
//			fin.ROE5 = fin.
//				ReturnOnEquity(last5)

			fin.DebtToFCF1 = fin.DebtToFreeCashFlow(last1)
			fin.DebtToFCF2 = fin.DebtToFreeCashFlow(last2)
			fin.DebtToFCF3 = fin.DebtToFreeCashFlow(last3)
			fin.DebtToFCF4 = fin.DebtToFreeCashFlow(last4)
			fin.DebtToFCF5 = fin.DebtToFreeCashFlow(last5)

			fin.DebtToEqu1 = fin.DebtToEquity(last1)
			fin.DebtToEqu2 = fin.DebtToEquity(last2)
			fin.DebtToEqu3 = fin.DebtToEquity(last3)
			fin.DebtToEqu4 = fin.DebtToEquity(last4)
			fin.DebtToEqu5 = fin.DebtToEquity(last5)

			fin.CorToRevTTM = fin.IncomeStatement.
				CostOfRevenueToRevenue(lastTTM)
			fin.CorToRev1 = fin.IncomeStatement.
				CostOfRevenueToRevenue(last1)
			fin.CorToRev2 = fin.IncomeStatement.
				CostOfRevenueToRevenue(last2)
			fin.CorToRev3 = fin.IncomeStatement.
				CostOfRevenueToRevenue(last3)
			fin.CorToRev4 = fin.IncomeStatement.
				CostOfRevenueToRevenue(last4)
			fin.CorToRev5 = fin.IncomeStatement.
				CostOfRevenueToRevenue(last5)

            financials = append(financials, fin)
		}
	}

	sort.SliceStable(
		financials,
		func(i, j int) bool {
			v0 := financials[i].ROIC1
            if math.IsNaN(v0) {
                v0 = 0;
            }
			v1 := financials[j].ROIC1
            if math.IsNaN(v1) {
                v1 = 0;
            }
			return v0 > v1

			/*
				pe0 := financials[i].Valuation.
					PriceToEarnings("Current")
				pe1 := financials[j].Valuation.
					PriceToEarnings("Current")
				return pe0 < pe1
			*/
		},
	)

	c.printFinancials(financials)
	return nil
}

func (c *Command) printFinancials(
	financials []*financials,
) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(
		buf, 0, 0, 2, ' ', tabwriter.AlignRight)

	p := message.NewPrinter(language.English)

	b := &bytes.Buffer{}
	b.WriteString(fmt.Sprintf("%-10v", "Symbol"))
	b.WriteByte('\t')

	b.WriteString("MCap")
	b.WriteByte('\t')

//	b.WriteString("MyPE")
//	b.WriteByte('\t')
	b.WriteString("PE")
	b.WriteByte('\t')
//	b.WriteString("PB")
//	b.WriteByte('\t')
//	b.WriteString("NetCash/MCap")
//	b.WriteByte('\t')

	b.WriteString("ROIC1%")
	b.WriteByte('\t')
	b.WriteString("ROIC2%")
	b.WriteByte('\t')
	b.WriteString("ROIC3%")
	b.WriteByte('\t')
	b.WriteString("ROIC4%")
	b.WriteByte('\t')
	b.WriteString("ROIC5%")
	b.WriteByte('\t')

//	b.WriteString("ROE1%")
//	b.WriteByte('\t')
//	b.WriteString("ROE2%")
//	b.WriteByte('\t')
//	b.WriteString("ROE3%")
//	b.WriteByte('\t')
//	b.WriteString("ROE4%")
//	b.WriteByte('\t')
//	b.WriteString("ROE5%")
//	b.WriteByte('\t')

	b.WriteString("Debt/Equ1")
	b.WriteByte('\t')
	b.WriteString("Debt/Equ2")
	b.WriteByte('\t')
	b.WriteString("Debt/Equ3")
	b.WriteByte('\t')
	b.WriteString("Debt/Equ4")
	b.WriteByte('\t')
	b.WriteString("Debt/Equ5")
	b.WriteByte('\t')

	b.WriteString("Debt/FCF1")
	b.WriteByte('\t')
	b.WriteString("Debt/FCF2")
	b.WriteByte('\t')
	b.WriteString("Debt/FCF3")
	b.WriteByte('\t')
	b.WriteString("Debt/FCF4")
	b.WriteByte('\t')
	b.WriteString("Debt/FCF5")
	b.WriteByte('\t')

	b.WriteString("Cap rate%")
	b.WriteByte('\t')
	b.WriteString("FCFPS CAGR%")
	b.WriteByte('\t')
	b.WriteString("FCFPSTTM")
	b.WriteByte('\t')
	b.WriteString("FCFPS1")
	b.WriteByte('\t')
	b.WriteString("FCFPS2")
	b.WriteByte('\t')
	b.WriteString("FCFPS3")
	b.WriteByte('\t')
	b.WriteString("FCFPS4")
	b.WriteByte('\t')
	b.WriteString("FCFPS5")
	b.WriteByte('\t')


	b.WriteString("BVDivPS CAGR%")
	b.WriteByte('\t')
	b.WriteString("BVPS1")
	b.WriteByte('\t')
	b.WriteString("BVPS2")
	b.WriteByte('\t')
	b.WriteString("BVPS3")
	b.WriteByte('\t')
	b.WriteString("BVPS4")
	b.WriteByte('\t')
	b.WriteString("BVPS5")
	b.WriteByte('\t')

	b.WriteString("DivPS1")
	b.WriteByte('\t')
	b.WriteString("DivPS2")
	b.WriteByte('\t')
	b.WriteString("DivPS3")
	b.WriteByte('\t')
	b.WriteString("DivPS4")
	b.WriteByte('\t')
	b.WriteString("DivPS5")
	b.WriteByte('\t')

	b.WriteString("COR/RevTTM")
	b.WriteByte('\t')
	b.WriteString("COR/Rev1")
	b.WriteByte('\t')
	b.WriteString("COR/Rev2")
	b.WriteByte('\t')
	b.WriteString("COR/Rev3")
	b.WriteByte('\t')
	b.WriteString("COR/Rev4")
	b.WriteByte('\t')
	b.WriteString("COR/Rev5")
	b.WriteByte('\t')

	fmt.Fprintln(w, b.String())

	for _, v := range financials {
		if !filterCapRate10(v) {
		    //continue
		}

        if !filterFCFPSGrowing(v) {
		    continue
		}

        if !filterBVPSGrowing(v) {
		    continue
		}

        if !filterDebtToEquity1Low(v) {
		    //continue
		}

		b.Reset()
		b.WriteString(fmt.Sprintf(
			"%-10v",
			v.Exchange+"/"+v.Symbol,
		))
		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%s", v.Realtime.MarketCapStr()))
		b.WriteByte('\t')

//		b.WriteString(p.Sprintf("%.2f", v.PToFCFTTM))
//		b.WriteByte('\t')
//
        pe := v.Valuation.PriceToEarnings("Current")
		b.WriteString(p.Sprintf("%.2f", pe))
		b.WriteByte('\t')
//
//		pb := v.Valuation.PriceToBook("Current")
//		b.WriteString(p.Sprintf("%.2f", pb))
//		b.WriteByte('\t')
//
//		b.WriteString(p.Sprintf("%.2f", v.NetCashToMCap))
//		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%.2f", v.ROIC1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.ROIC2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.ROIC3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.ROIC4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.ROIC5))
		b.WriteByte('\t')

//		b.WriteString(p.Sprintf("%.2f", v.ROE1))
//		b.WriteByte('\t')
//		b.WriteString(p.Sprintf("%.2f", v.ROE2))
//		b.WriteByte('\t')
//		b.WriteString(p.Sprintf("%.2f", v.ROE3))
//		b.WriteByte('\t')
//		b.WriteString(p.Sprintf("%.2f", v.ROE4))
//		b.WriteByte('\t')
//		b.WriteString(p.Sprintf("%.2f", v.ROE5))
//		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%.2f", v.DebtToEqu1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToEqu2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToEqu3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToEqu4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToEqu5))
		b.WriteByte('\t')


		b.WriteString(p.Sprintf("%.2f", v.DebtToFCF1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToFCF2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToFCF3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToFCF4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DebtToFCF5))
		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%.2f", v.CapRate()))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FreeCashFlowPerShareCAGR()))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FCFPS1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FCFPS1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FCFPS2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FCFPS3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FCFPS4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.FCFPS5))
		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%.2f", v.BookValueDividendPerShareCAGR()))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.BVPS1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.BVPS2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.BVPS3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.BVPS4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.BVPS5))
		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%.2f", v.DivPS1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DivPS2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DivPS3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DivPS4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.DivPS5))
		b.WriteByte('\t')

		b.WriteString(p.Sprintf("%.2f", v.CorToRevTTM))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.CorToRev1))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.CorToRev2))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.CorToRev3))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.CorToRev4))
		b.WriteByte('\t')
		b.WriteString(p.Sprintf("%.2f", v.CorToRev5))
		b.WriteByte('\t')


		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", buf.String())
}

func filterROICPositive(v *financials) bool {
	return v.ROIC1 > 0 //&&
		//v.ROIC2 > 0 &&
		//v.ROIC3 > 0 &&
		//v.ROIC4 > 0 //&&
		//v.ROIC5 > 0
}

func filterFCFPSGrowing(v *financials) bool {
	return v.FCFPSTTM > v.FCFPS5 &&
		v.FCFPSTTM > 0 &&
		v.FCFPS1 > 0 &&
		v.FCFPS2 > 0 &&
		v.FCFPS3 > 0 &&
		v.FCFPS4 > 0 &&
		v.FCFPS5 > 0
}

func filterBVPSGrowing(v *financials) bool {
	return v.BVPS1 > v.BVPS5 &&
		v.BVPS1 > 0 &&
		v.BVPS2 > 0 &&
		v.BVPS3 > 0 &&
		v.BVPS4 > 0 &&
		v.BVPS5 > 0
}

func filterEarningsGrowing(v *financials) bool {
	return v.E1 > v.E2 &&
		v.E2 > v.E3 &&
		v.E3 > v.E4 &&
		v.E4 > 0 //v.E5 &&
        //v.E5 > 0
}

func filterDebtToEquity1Low(v *financials) bool {
	return v.DebtToEqu1 <= 0.4
}

func filterCapRate10(v *financials) bool {
	return v.CapRate() >= 10
}

func (c *Command) financials(
	ctx context.Context,
	dir string,
) (*financials, error) {
	file := filepath.Join(dir, "is.json")
	exist, err := exists(file)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	var incomeStatement statement
	err = c.decodeJSON(file, &incomeStatement)
	if err != nil {
		return nil, err
	}

	file = filepath.Join(dir, "bs.json")
	exist, err = exists(file)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	var balanceSheet statement
	err = c.decodeJSON(file, &balanceSheet)
	if err != nil {
		return nil, err
	}

	file = filepath.Join(dir, "cf.json")
	exist, err = exists(file)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	var cashFlow statement
	err = c.decodeJSON(file, &cashFlow)
	if err != nil {
		return nil, err
	}

	file = filepath.Join(dir, "valuation.json")
	exist, err = exists(file)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	var valuation valuation
	err = c.decodeJSON(file, &valuation)
	if err != nil {
		return nil, err
	}

	file = filepath.Join(dir, "rt.json")
	exist, err = exists(file)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	var realtime realtime
	err = c.decodeJSON(file, &realtime)
	if err != nil {
		return nil, err
	}

	financials := &financials{
		IncomeStatement: &incomeStatement,
		BalanceSheet:    &balanceSheet,
		CashFlow:        &cashFlow,
		Valuation:       &valuation,
		Realtime:        &realtime,
	}

	return financials, nil
}

func (c *Command) decodeJSON(
	file string,
	statement interface{},
) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	err = dec.Decode(statement)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

type financials struct {
	Exchange        string
	Symbol          string
	IncomeStatement *statement
	BalanceSheet    *statement
	CashFlow        *statement
	Valuation       *valuation
	Realtime        *realtime

    PToFCFTTM float64

	NetCashToMCap float64

    // Free cash flow per share
	FCFPSTTM float64
	FCFPS1 float64
	FCFPS2 float64
	FCFPS3 float64
	FCFPS4 float64
	FCFPS5 float64

    // Book value (equity) per share
	BVPS1 float64
	BVPS2 float64
	BVPS3 float64
	BVPS4 float64
	BVPS5 float64

    // Dividend per share
    DivPS1 float64
	DivPS2 float64
	DivPS3 float64
	DivPS4 float64
	DivPS5 float64

	E1 float64
	E2 float64
	E3 float64
	E4 float64
	E5 float64

    // Earnings per share
	EPS1 float64
	EPS2 float64
	EPS3 float64
	EPS4 float64
	EPS5 float64

    // Operating cash flow per share
	OCFPS1 float64
	OCFPS2 float64
	OCFPS3 float64
	OCFPS4 float64
	OCFPS5 float64

    // Sales per share
	SPS1 float64
	SPS2 float64
	SPS3 float64
	SPS4 float64
	SPS5 float64

    ROIC1 float64
	ROIC2 float64
	ROIC3 float64
	ROIC4 float64
	ROIC5 float64

	ROE1 float64
	ROE2 float64
	ROE3 float64
	ROE4 float64
	ROE5 float64

	DebtToFCF1 float64
	DebtToFCF2 float64
	DebtToFCF3 float64
	DebtToFCF4 float64
	DebtToFCF5 float64

	DebtToEqu1 float64
	DebtToEqu2 float64
	DebtToEqu3 float64
	DebtToEqu4 float64
	DebtToEqu5 float64

    CorToRevTTM float64
    CorToRev1 float64
	CorToRev2 float64
	CorToRev3 float64
	CorToRev4 float64
	CorToRev5 float64
}

func (f *financials) NetCashToMarketCap(
	period string,
) float64 {
	cash := f.BalanceSheet.CashAndCashEquivalents(period)
	totLia := f.BalanceSheet.Liabilities(period)
	if f.Realtime.MarketCap == 0 {
		return 0
	}
	return (cash - math.Abs(totLia)) /
		f.Realtime.MarketCap
}

func (f *financials) ReturnOnInvestedCapital(
	period string,
) float64 {
	//ear := f.CashFlow.FreeCashFlow(period)
	ear := f.IncomeStatement.OperatingIncome(period)
	if ear <= 0 {
		return math.NaN()
	}
	cap := f.BalanceSheet.InvestedCapital(period)
	if cap <= 0 {
		return math.NaN()
	}
	return (ear / cap) * 100
}

func (f *financials) ReturnOnEquity(
	period string,
) float64 {
	//ear := f.CashFlow.FreeCashFlow(period)
	ear := f.IncomeStatement.OperatingIncome(period)
	if ear <= 0 {
		return math.NaN()
	}
	equ := f.BalanceSheet.Equity(period)
    //fmt.Println(period, "equ", equ)
	if equ <= 0 {
		return math.NaN()
	}
	return (ear / equ) * 100
}

func (f *financials) DebtToFreeCashFlow(
	period string,
) float64 {
	debt := f.BalanceSheet.Debt(period)
	fcf := f.CashFlow.FreeCashFlow(period)
	if fcf <= 0 {
		return math.NaN()
	}
	return debt / fcf
}

func (f *financials) FreeCashFlowPerShareCAGR() float64 {
    n := 5
	to := f.FCFPSTTM
    from := f.FCFPS5
    if to <= 0 || from <= 0 {
		return math.NaN()
	}
    return (math.Pow(to / from, float64(1) / float64(n)) - 1) * 100
}

func (f *financials) CapRate() float64 {
    return (f.FCFPSTTM / f.Realtime.LastPrice) * 100
}

func (f *financials) FreeCashFlowPerShare(
	period string,
) float64 {
	sha := f.IncomeStatement.SharesOutstanding(period)
	fcf := f.CashFlow.FreeCashFlow(period)
	if sha <= 0 {
		return math.NaN()
	}
	return fcf / sha
}

func (f *financials) BookValueDividendPerShareCAGR() float64 {
    n := 4
	to := f.BVPS1 + math.Abs(f.DivPS1)
    from := f.BVPS5 + math.Abs(f.DivPS5)
	if to <= 0 || from <= 0 {
		return math.NaN()
	}
    return (math.Pow(to / from, float64(1) / float64(n)) - 1) * 100
}

func (f *financials) BookValuePerShare(
	period string,
) float64 {
	sha := f.IncomeStatement.SharesOutstanding(period)
	equ := f.BalanceSheet.Equity(period)
	if sha <= 0 {
		return math.NaN()
	}
	return equ / sha
}

func (f *financials) DividendPerShare(
	period string,
) float64 {
	sha := f.IncomeStatement.SharesOutstanding(period)
	div := f.CashFlow.DividendPaid(period)
	if sha <= 0 {
		return math.NaN()
	}
	return div / sha
}

type statement struct {
	ColumnDefs []string             `json:"columnDefs"`
	Rows       []*statementSubLevel `json:"rows"`
	Footer     *statementFooter     `json:"footer"`
}

func (s *statement) OrderOfMagnitude() float64 {
	switch s.Footer.OrderOfMagnitude {
	case "Billion":
		return 1000000000
	case "Million":
		return 1000000
	case "Thousand":
		return 1000
	default:
		panic("unexpected order of magnitude: " + s.Footer.OrderOfMagnitude)
	}
}

type statementSubLevel struct {
	Label     string               `json:"label"`
	SubLevels []*statementSubLevel `json:"subLevel"`
	Datum     []interface{}        `json:"datum"`
}

type statementFooter struct {
	Currency          string `json:"currency"`
	CurrencySymbol    string `json:"currencySymbol"`
	OrderOfMagnitude  string `json:"orderOfMagnitude"`
	FiscalYearEndDate string `json:"fiscalYearEndDate"`
}

func (s *statement) SharesOutstanding(
	period string,
) float64 {
	return s.value(
		s.periodIndex(period),
		"Diluted Weighted Average Shares Outstanding",
		s.Rows,
	)
}

func (s *statement) Revenue(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
    // banks
	netIntInc := s.NetInterestIncome(period)
	if netIntInc > 0 {
		nonIntInc := s.NonInterestIncome(period)
		return netIntInc + nonIntInc
	}

    // others
	return s.value(
		s.periodIndex(period),
		"Total Revenue",
		s.Rows,
	)
}

func (s *statement) CostOfRevenue(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	return s.value(
		s.periodIndex(period),
		"Cost of Revenue",
		s.Rows,
	)
}

func (s *statement) CostOfRevenueToRevenue(
	period string,
) float64 {
    cor := s.CostOfRevenue(period)
    rev := s.Revenue(period)
    if rev <= 0 {
        return math.NaN()
    }
    return math.Abs(cor) / rev
}

func (s *statement) GrossIncome(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
    // banks, no cost of goods sold
	netIntInc := s.NetInterestIncome(period)
	if netIntInc > 0 {
		return s.Revenue(period)
	}

	// other companies
	return s.value(
		s.periodIndex(period),
		"Gross Profit",
		s.Rows,
	)
}

func (s *statement) OperatingIncome(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	// non bank companies
    inc := s.value(
		s.periodIndex(period),
		"Total Operating Profit/Loss",
		s.Rows,
	)

    if inc != 0 {
        return inc
    }

	// banks
	nonIntExp := s.NonInterestExpenses(period)
	if nonIntExp < 0 {
		gro := s.GrossIncome(period)
		return gro + nonIntExp
	}

    return 0
}

func (s *statement) NetInterestIncome(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	return s.value(
		s.periodIndex(period),
		"Net Interest Income",
		s.Rows,
	)
}

func (s *statement) NonInterestIncome(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	return s.value(
		s.periodIndex(period),
		"Non-Interest Income",
		s.Rows,
	)
}

func (s *statement) NonInterestExpenses(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	return s.value(
		s.periodIndex(period),
		"Non-Interest Expenses",
		s.Rows,
	)
}

func (s *statement) OperatingEfficiency(
	period string,
) float64 {
	exp := s.NonInterestExpenses(period)
	netIntInc := s.NetInterestIncome(period)
	nonIntInc := s.NonInterestIncome(period)
	//fmt.Println(period, netIntInc, nonIntInc, exp)
	return (math.Abs(exp) / (netIntInc + nonIntInc)) * 100
}

func (s *statement) NetIncome(period string) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	return s.value(
		s.periodIndex(period),
		"Net Income Available to Common Stockholders",
		s.Rows,
	)
}

func (s *statement) DilutedSharesOutstanding(period string) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
    return s.value(
		s.periodIndex(period),
        "Diluted Weighted Average Shares Outstanding",
		s.Rows,
	)
}

func (s *statement) InvestedCapital(
	period string,
) float64 {
	equ := s.Equity(period)
	debt := s.Debt(period)
	return equ + debt
}

func (s *statement) Equity(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }

    return s.value(
		s.periodIndex(period),
		"Total Equity",
		s.Rows,
	)
}

func (s *statement) Debt(
	period string,
) float64 {
    if len(s.Rows) == 0 {
        return 0
    }
	curr := s.value(
		s.periodIndex(period),
		"Current Debt and Capital Lease Obligation",
		s.Rows,
	)

	long := s.value(
		s.periodIndex(period),
		"Long Term Debt and Capital Lease Obligation",
		s.Rows,
	)

	//fmt.Printf("Debt %v %f %f\n", period, curr, long)
	// non banks
	if curr > 0 || long > 0 {
		return curr + long
	}

	// banks
	return s.value(
		s.periodIndex(period),
		"Debt and Capital Lease Obligations",
		s.Rows,
	)
}

func (f *financials) DebtToEquity(
	period string,
) float64 {
	debt := f.BalanceSheet.Debt(period)
	equ  := f.BalanceSheet.Equity(period)
    if equ <= 0 {
        return math.NaN()
    }
	return debt / equ
}

func (s *statement) LiabilitiesNoDeposits(
	period string,
) float64 {
	totLia := s.Liabilities(period)
	totDep := s.Deposits(period)
	return totLia - totDep
}

func (s *statement) Liabilities(
	period string,
) float64 {
	return s.value(
		s.periodIndex(period),
		"Total Liabilities",
		s.Rows,
	)
}

func (s *statement) Deposits(
	period string,
) float64 {
	return s.value(
		s.periodIndex(period),
		"Total Deposits",
		s.Rows,
	)
}

func (s *statement) CashAndCashEquivalents(
	period string,
) float64 {
	return s.value(
		s.periodIndex(period),
		"Cash and Cash Equivalents",
		s.Rows,
	)
}

func (s *statement) OperatingCashFlow(
	period string,
) float64 {
	if len(s.Rows) == 0 {
		return 0
	}

	dir := s.value(
		s.periodIndex(period),
		"Net Cash Flow from Continuing Operating Activities, Direct",
		s.Rows,
	)

	ind := s.value(
		s.periodIndex(period),
		"Net Cash Flow from Continuing Operating Activities, Indirect",
		s.Rows,
	)

	return dir + ind
}

func (s *statement) FreeCashFlow(
	period string,
) float64 {
	if len(s.Rows) == 0 {
		return 0
	}

	opCash := s.OperatingCashFlow(period)

	pppe := s.value(
		s.periodIndex(period),
		"Purchase of Property, Plant and Equipment",
		s.Rows,
	)

	capEx := s.value(
		s.periodIndex(period),
		"Capital Expenditure, Reported",
		s.Rows,
	)

	fcf := opCash + capEx
	if pppe < 0 {
		fcf += pppe
	}
	return fcf
}

func (s *statement) DividendPaid(
	period string,
) float64 {
	if len(s.Rows) == 0 {
		return 0
	}

	return s.value(
		s.periodIndex(period),
        "Common Stock Dividends Paid",
		s.Rows,
	)
}

func (s *statement) periodIndex(period string) int {
	for i, v := range s.ColumnDefs {
		if v == period {
			return i
		}
	}
	return -1
}

func (s *statement) value(
	periodIndex int,
	label string,
	rows []*statementSubLevel,
) float64 {
	if periodIndex == -1 {
		return 0
	}

	levels := make([]*statementSubLevel, 0)
	levels = append(levels, rows...)

	for len(levels) > 0 {
		next := levels[0]
		levels = levels[1:]

		if next.Label == label {
			v := next.Datum[periodIndex]
			num, _ := v.(float64)
			return num * s.OrderOfMagnitude()
		}

		levels = append(levels, next.SubLevels...)
	}
	return 0
}

type realtime struct {
	MarketCap float64 `json:"marketCap"`
    LastPrice float64 `json:"lastPrice"`

}

const trillion = float64(1000000000000);
const billion = float64(1000000000);
const million = float64(1000000);
const thousand = float64(1000);

func (r *realtime) MarketCapStr() string {
    if r.MarketCap > trillion {
        v := r.MarketCap / trillion
        return strconv.FormatFloat(v, 'f', 3, 64) + "tr"
    }
    if r.MarketCap > billion {
        v := r.MarketCap / billion
        return strconv.FormatFloat(v, 'f', 3, 64) + "b"
    }

    if r.MarketCap > million {
        v := r.MarketCap / million
        return strconv.FormatFloat(v, 'f', 3, 64) + "m"
    }

    v := r.MarketCap / thousand
    return strconv.FormatFloat(v, 'f', 3, 64) + "th"
}

type valuation struct {
	Collapsed *valuationCollapsed `json:"Collapsed"`
}

type valuationCollapsed struct {
	ColumnDefs []string        `json:"columnDefs"`
	Rows       []*valuationRow `json:"rows"`
}

type valuationRow struct {
	Label string        `json:"label"`
	Datum []interface{} `json:"datum"`
}

func (s *valuation) periodIndex(period string) int {
	for i, v := range s.Collapsed.ColumnDefs {
		if v == period {
			return i - 1
		}
	}
	return -1
}

func (s *valuation) value(
	periodIndex int,
	label string,
) float64 {
	if periodIndex == -1 {
		return 0
	}

	for _, row := range s.Collapsed.Rows {
		if row.Label == label {
			d := row.Datum[periodIndex]
			str, _ := d.(string)
			if str == "" {
				return 0
			}
			num, err := strconv.ParseFloat(str, 64)
			if err != nil {
				panic("parse float: " + err.Error())
			}
			return num
		}
	}
	return 0
}

func (s *valuation) PriceToEarnings(
	period string,
) float64 {
	return s.value(
		s.periodIndex(period),
		"Price/Earnings",
	)
}

func (s *valuation) PriceToBook(
	period string,
) float64 {
	return s.value(
		s.periodIndex(period),
		"Price/Book",
	)
}

func morningstarURL(
	u string,
	tail string,
) (string, string, string) {
	parts := strings.Split(u, "/")
	exch := strings.ToUpper(parts[4])
	symbol := strings.ToUpper(parts[5])
	u = strings.Join(parts[0:6], "/")
	u += "/" + tail
	return u, symbol, exch
}

func morningstarURLValuation(u string) (string, string, string) {
	return morningstarURL(u, "valuation")
}

func morningstarURLFinancials(u string) (string, string, string) {
	return morningstarURL(u, "financials")
}

func exists(f string) (bool, error) {
	exists := true
	_, err := os.Stat(f)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		exists = false
	}
	return exists, nil
}

func (c *Command) pullValuation(ctx context.Context) error {
	baseDir := c.opts.dir
	if baseDir == "" {
		return fmt.Errorf("dir must be specified")
	}

	exist, err := exists(baseDir)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("dir not found: %v", baseDir)
	}

	urlsFile := c.args[0]
	uf, err := os.Open(urlsFile)
	if err != nil {
		return err
	}
	defer uf.Close()

	jobCh, resCh := c.opts.financialsService.PullValuation(
		ctx,
		&divyield.FinancialsPullValuationInput{},
	)

	go func() {
		scanner := bufio.NewScanner(uf)
	SCANNER:
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				break SCANNER
			default:
				// noop
			}

			u := scanner.Text()
			u = strings.TrimSpace(u)
			if u == "" || strings.HasPrefix(u, "#") {
				continue
			}

			_, symbol, _ := morningstarURLValuation(u)

			/*
				m := filepath.Join(baseDir, exch, symbol, "missing")
				exist, err = exists(m)
				if err != nil {
					fmt.Println(err)
					continue
				}
				if exist {
					continue
				}
			*/

			/*
				dir := filepath.Join(baseDir, exch, symbol, "is.json")
				exist, err = exists(dir)
				if err != nil {
					fmt.Println(err)
					continue
				}
				if exist {
					continue
				}
			*/

			jobCh <- u
			fmt.Printf("%v: %v\n", symbol, u)
		}
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
		close(jobCh)
	}()

	for res := range resCh {
		_, symbol, exch := morningstarURLValuation(res.URL)
		err := os.MkdirAll(
			filepath.Join(baseDir, exch, symbol),
			0777,
		)
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			continue
		}

		err = res.Err
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			if strings.Contains(
				err.Error(),
				"deadline exceeded",
			) {
				err := ioutil.WriteFile(
					missingFile(baseDir, res.URL),
					[]byte(""),
					0644,
				)
				if err != nil {
					fmt.Printf("%v: %v\n", symbol, err)
				}
			}
			continue
		}

		err = ioutil.WriteFile(
			realtimeFile(baseDir, res.URL),
			[]byte(res.Realtime),
			0644,
		)
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			continue
		}

		err = ioutil.WriteFile(
			valuationFile(baseDir, res.URL),
			[]byte(res.Valuation),
			0644,
		)
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			continue
		}

		err = ioutil.WriteFile(
			incomeStatementFile(baseDir, res.URL),
			[]byte(res.IncomeStatement),
			0644,
		)
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			continue
		}

		err = ioutil.WriteFile(
			balanceSheetFile(baseDir, res.URL),
			[]byte(res.BalanceSheet),
			0644,
		)
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			continue
		}

		err = ioutil.WriteFile(
			cashFlowFile(baseDir, res.URL),
			[]byte(res.CashFlow),
			0644,
		)
		if err != nil {
			fmt.Printf("%v: %v\n", symbol, err)
			continue
		}
	}
	return nil
}

func writeFile(p string, records [][]string) error {
	err := os.MkdirAll(filepath.Dir(p), 0777)
	if err != nil {
		return err
	}
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return writeCSV(f, records)
}

func missingFile(baseDir, u string) string {
	_, symbol, exch := morningstarURLValuation(u)
	return filepath.Join(baseDir, exch, symbol, "missing")
}

func realtimeFile(baseDir, u string) string {
	_, symbol, exch := morningstarURLValuation(u)
	return filepath.Join(baseDir, exch, symbol, "rt.json")
}

func valuationFile(baseDir, u string) string {
	_, symbol, exch := morningstarURLValuation(u)
	return filepath.Join(baseDir, exch, symbol, "valuation.json")
}

func incomeStatementFile(baseDir, u string) string {
	_, symbol, exch := morningstarURLValuation(u)
	return filepath.Join(baseDir, exch, symbol, "is.json")
}

func balanceSheetFile(baseDir, u string) string {
	_, symbol, exch := morningstarURLValuation(u)
	return filepath.Join(baseDir, exch, symbol, "bs.json")
}

func cashFlowFile(baseDir, u string) string {
	_, symbol, exch := morningstarURLValuation(u)
	return filepath.Join(baseDir, exch, symbol, "cf.json")
}

func writeCSV(o io.Writer, records [][]string) error {
	w := csv.NewWriter(o)
	w.Comma = ';'
	w.WriteAll(records)
	err := w.Error()
	if err != nil {
		return fmt.Errorf("write csv: %v", err)
	}
	return nil
}

func readCSV(in io.Reader) ([][]string, error) {
	r := csv.NewReader(in)
	r.Comma = ';'
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv: %v", err)
	}
	return records, nil
}

func (c *Command) pull(ctx context.Context) error {
	var err error
	from := c.opts.startDate

	symbols, err := c.resolveSymbols(ctx, c.args)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("Symbol not found")
	}

	err = c.opts.db.InitSchema(ctx, symbols)
	if err != nil {
		return fmt.Errorf("init schema: %v", err)
	}

	eout, err := c.opts.exchangeService.Fetch(
		ctx,
		&divyield.ExchangeFetchInput{},
	)
	if err != nil {
		return err
	}

	for _, symbol := range symbols {
		utd, err := c.upToDate(ctx, symbol)
		if err != nil {
			return fmt.Errorf(
				"%v: check up to date: %v",
				symbol,
				err,
			)
		}
		if utd {
			c.writef("%v: up to date", symbol)
			continue
		}

		pullStart := time.Now()

		proout, err := c.opts.profileService.Fetch(
			ctx,
			&divyield.ProfileFetchInput{
				Symbol: symbol,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: %v", symbol, err)
		}

		profile := proout.Profile
		if profile == nil {
			return fmt.Errorf(
				"%v: profile not found",
				symbol,
			)
		}

		var priceCurrency string
		dashIdx := strings.LastIndexByte(symbol, '-')
		if dashIdx != -1 {
			symbolSuffix := symbol[dashIdx:]
			for _, ex := range eout.Exchanges {
				if ex.Suffix == symbolSuffix {
					priceCurrency = ex.Currency
				}
			}
			if priceCurrency == "" {
				return fmt.Errorf(
					"%v: currency not found: %v",
					symbol,
					symbolSuffix,
				)
			}
		} else {
			priceCurrency = "USD"
		}

		fromSplits := from
		if !c.opts.reset {
			fromSplits, err = c.adjustFromSplits(
				ctx,
				symbol,
				from,
			)
			if err != nil {
				return fmt.Errorf("%v: %v", symbol, err)
			}
		}

		sout, err := c.opts.splitService.Fetch(
			ctx,
			&divyield.SplitFetchInput{
				Symbol: symbol,
				From:   fromSplits,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: %v", symbol, err)
		}
		c.writef("%v: %v splits", symbol, len(sout.Splits))

		_, err = c.opts.db.SaveSplits(
			ctx,
			&divyield.DBSaveSplitsInput{
				Symbol: symbol,
				Splits: sout.Splits,
				Reset:  c.opts.reset,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: save splits: %v", symbol, err)
		}

		fromDividends := from
		if !c.opts.reset {
			fromDividends, err = c.adjustFromDividends(
				ctx,
				symbol,
				from,
			)
			if err != nil {
				return fmt.Errorf("%v: %v", symbol, err)
			}
		}

		dout, err := c.opts.dividendService.Fetch(
			ctx,
			&divyield.DividendFetchInput{
				Symbol: symbol,
				From:   fromDividends,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: %v", symbol, err)
		}
		for _, v := range dout.Dividends {
			if v.Currency != priceCurrency {
				ccout, err := c.opts.currencyService.Convert(
					ctx,
					&divyield.CurrencyConvertInput{
						From:   v.Currency,
						To:     priceCurrency,
						Amount: v.Amount,
						Date:   v.ExDate,
					},
				)
				if err != nil {
					return fmt.Errorf("%v: %v", symbol, err)
				}

				v.Currency = priceCurrency
				v.Amount = ccout.Amount
			}
		}
		c.writef(
			"%v: %v dividends",
			symbol,
			len(dout.Dividends),
		)
		_, err = c.opts.db.SaveDividends(
			ctx,
			&divyield.DBSaveDividendsInput{
				Symbol:    symbol,
				Dividends: dout.Dividends,
				Reset:     c.opts.reset,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: save dividends: %v", symbol, err)
		}

		fromPrices := from
		if !c.opts.reset {
			fromPrices, err = c.adjustFromPrices(
				ctx,
				symbol,
				from,
			)
			if err != nil {
				return fmt.Errorf("%v: %v", symbol, err)
			}
		}

		pout, err := c.opts.priceService.Fetch(
			ctx,
			&divyield.PriceFetchInput{
				Symbol: symbol,
				From:   fromPrices,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: %v", symbol, err)
		}
		for _, v := range pout.Prices {
			v.Currency = priceCurrency
		}
		c.writef("%v: %v prices", symbol, len(pout.Prices))

		_, err = c.opts.db.SavePrices(
			ctx,
			&divyield.DBSavePricesInput{
				Symbol: symbol,
				Prices: pout.Prices,
				Reset:  c.opts.reset,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: save prices: %v", symbol, err)
		}

		profile.Pulled = pullStart
		_, err = c.opts.db.SaveProfile(
			ctx,
			&divyield.DBSaveProfileInput{
				Symbol:  symbol,
				Profile: profile,
			},
		)
		if err != nil {
			return fmt.Errorf("%v: save profile: %v", symbol, err)
		}

	}
	return nil
}

func (c *Command) upToDate(
	ctx context.Context,
	symbol string,
) (bool, error) {
	out, err := c.opts.db.Profiles(
		ctx,
		&divyield.DBProfilesInput{
			Symbols: []string{symbol},
		},
	)
	if err != nil {
		return false, err
	}

	if len(out.Profiles) == 0 {
		return false, nil
	}

	today := date(time.Now())
	pulledDate := date(out.Profiles[0].Pulled)
	return !c.opts.force && pulledDate.Equal(today), nil
}

func date(t time.Time) time.Time {
	return time.Date(
		t.Year(), t.Month(), t.Day(),
		0, 0, 0, 0, t.Location(),
	)
}

const symbolPatternChar = "%"
const symbolPatternExclude = "-"

func (c *Command) resolveSymbols(
	ctx context.Context,
	symbols []string,
) ([]string, error) {
	out, err := c.opts.db.Profiles(
		ctx,
		&divyield.DBProfilesInput{},
	)
	if err != nil {
		return nil, err
	}

	symbolsDB := make([]string, 0)
	for _, p := range out.Profiles {
		symbolsDB = append(symbolsDB, p.Symbol)
	}

	if len(symbols) == 0 {
		return symbolsDB, nil
	}

	for i, v := range symbols {
		symbols[i] = strings.ToUpper(v)
	}

	symbolsMap := make(map[string]struct{})
	excludeMap := make(map[string]struct{})
	for _, v := range symbols {
		exclude := false
		if strings.HasPrefix(v, symbolPatternExclude) {
			exclude = true
			v = v[1:]
		}

		if strings.HasSuffix(v, symbolPatternChar) {
			prefix := strings.TrimRight(v, symbolPatternChar)
			for _, sdb := range symbolsDB {
				if strings.HasPrefix(sdb, prefix) {
					if exclude {
						excludeMap[sdb] = struct{}{}
					} else {
						symbolsMap[sdb] = struct{}{}
					}
				}
			}
		} else {
			if exclude {
				excludeMap[v] = struct{}{}
			} else {
				symbolsMap[v] = struct{}{}
			}
		}
	}

	symbolsRes := make([]string, 0, len(symbolsMap))
	for v, _ := range symbolsMap {
		if _, ok := excludeMap[v]; !ok {
			symbolsRes = append(symbolsRes, v)
		}
	}
	sort.Strings(symbolsRes)
	return symbolsRes, nil
}

func toUpper(a []string) []string {
	for i, v := range a {
		a[i] = strings.ToUpper(v)
	}
	return a
}

func (c *Command) adjustFromSplits(
	ctx context.Context,
	symbol string,
	from time.Time,
) (time.Time, error) {
	latest, err := c.opts.db.Splits(
		ctx, symbol, &divyield.SplitFilter{Limit: 1})
	if err != nil {
		return time.Time{}, err
	}
	if len(latest) == 0 {
		return from, nil
	}
	return latest[0].ExDate.AddDate(0, 0, 1), nil
}

func (c *Command) adjustFromDividends(
	ctx context.Context,
	symbol string,
	from time.Time,
) (time.Time, error) {
	latest, err := c.opts.db.Dividends(
		ctx, symbol, &divyield.DividendFilter{Limit: 1})
	if err != nil {
		return time.Time{}, err
	}
	if len(latest) == 0 {
		return from, nil
	}
	return latest[0].ExDate.AddDate(0, 0, 1), nil
}

func (c *Command) adjustFromPrices(
	ctx context.Context,
	symbol string,
	from time.Time,
) (time.Time, error) {
	latest, err := c.opts.db.Prices(
		ctx, symbol, &divyield.PriceFilter{Limit: 1})
	if err != nil {
		return time.Time{}, err
	}
	if len(latest) == 0 {
		return from, nil
	}
	return latest[0].Date.AddDate(0, 0, 1), nil
}

func (c *Command) profile(ctx context.Context) error {
	symbols, err := c.resolveSymbols(ctx, c.args)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("Symbol not found")
	}
	symbol := symbols[0]
	in := &divyield.ProfileFetchInput{
		Symbol: symbol,
	}

	out, err := c.opts.profileService.Fetch(ctx, in)
	if err != nil {
		return err
	}

	if out.Profile == nil {
		c.writef("Not found: %v", in.Symbol)
		return nil
	}
	c.writeProfile(out.Profile)
	return nil
}

func (c *Command) writeProfile(cp *divyield.Profile) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}
	b.WriteString("Symbol:")
	b.WriteByte('\t')
	b.WriteString(cp.Symbol)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Name:")
	b.WriteByte('\t')
	b.WriteString(cp.Name)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Exchange:")
	b.WriteByte('\t')
	b.WriteString(cp.Exchange)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Issue type:")
	b.WriteByte('\t')
	b.WriteString(cp.IssueType)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Industry:")
	b.WriteByte('\t')
	b.WriteString(cp.Industry)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Sector:")
	b.WriteByte('\t')
	b.WriteString(cp.Sector)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Description:")
	b.WriteByte('\t')
	b.WriteString(cp.Description)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Website:")
	b.WriteByte('\t')
	b.WriteString(cp.Website)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Address:")
	b.WriteByte('\t')
	b.WriteString(cp.Address)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("State:")
	b.WriteByte('\t')
	b.WriteString(cp.State)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("City:")
	b.WriteByte('\t')
	b.WriteString(cp.City)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Zip:")
	b.WriteByte('\t')
	b.WriteString(cp.Zip)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Country:")
	b.WriteByte('\t')
	b.WriteString(cp.Country)
	fmt.Fprintln(w, b.String())

	b.Reset()
	b.WriteString("Phone:")
	b.WriteByte('\t')
	b.WriteString(cp.Phone)
	fmt.Fprintln(w, b.String())

	w.Flush()
	c.writef("%s", buf.String())
}

func (c *Command) symbols(ctx context.Context) error {
	in := &divyield.ISINResolveInput{
		ISIN: c.args[0],
	}

	out, err := c.opts.isinService.Resolve(ctx, in)
	if err != nil {
		return err
	}

	c.writeSymbolISINs(out.Symbols)
	return nil
}

func (c *Command) writeSymbolISINs(symbols []*divyield.SymbolISIN) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}
	b.WriteString("Region")
	b.WriteByte('\t')
	b.WriteString("Exchange")
	b.WriteByte('\t')
	b.WriteString("Symbol")
	fmt.Fprintln(w, b.String())

	for _, v := range symbols {
		b.Reset()
		b.WriteString(v.Region)
		b.WriteByte('\t')
		b.WriteString(v.Exchange)
		b.WriteByte('\t')
		b.WriteString(v.Symbol)
		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", buf.String())
}

func (c *Command) exchanges(ctx context.Context) error {
	in := &divyield.ExchangeFetchInput{}
	out, err := c.opts.exchangeService.Fetch(ctx, in)
	if err != nil {
		return err
	}

	c.writeExchanges(out.Exchanges)
	return nil
}

func (c *Command) writeExchanges(exchanges []*divyield.Exchange) {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)

	b := &bytes.Buffer{}
	b.WriteString("Region")
	b.WriteByte('\t')
	b.WriteString("Exchange")
	b.WriteByte('\t')
	b.WriteString("Suffix")
	b.WriteByte('\t')
	b.WriteString("Currency")
	b.WriteByte('\t')
	b.WriteString("Description")
	fmt.Fprintln(w, b.String())

	for _, v := range exchanges {
		b.Reset()
		b.WriteString(v.Region)
		b.WriteByte('\t')
		b.WriteString(v.Exchange)
		b.WriteByte('\t')
		b.WriteString(v.Suffix)
		b.WriteByte('\t')
		b.WriteString(v.Currency)
		b.WriteByte('\t')
		b.WriteString(v.Description)
		fmt.Fprintln(w, b.String())
	}

	w.Flush()
	c.writef("%s", buf.String())
}

func (c *Command) writef(format string, v ...interface{}) {
	if c.opts.writer != nil {
		fmt.Fprintf(c.opts.writer, format, v...)
	}
}

type statsGenerator struct {
	db                 divyield.DB
	writer             io.Writer
	startDate          time.Time
	inflation          *divyield.Inflation
	sp500DividendYield *divyield.SP500DividendYield

	divYieldFwdSP500Min float64
	divYieldFwdSP500Max float64
	divYieldTotalMin    float64
	ggrROI              float64
	ggrMin              float64
	ggrMax              float64
	noCutDividend       bool
	noDecliningDGR      bool
	dgrAvgMin           float64
	dgrYearly           bool
}

func (g *statsGenerator) divYieldFwdMin() float64 {
	return g.sp500DividendYield.Rate * g.divYieldFwdSP500Min
}

func (g *statsGenerator) divYieldFwdMax() float64 {
	return g.sp500DividendYield.Rate * g.divYieldFwdSP500Max
}

func (g *statsGenerator) Generate(
	ctx context.Context,
	symbols []string,
) (*divyield.Stats, error) {
	var workerWg sync.WaitGroup
	var resultWg sync.WaitGroup
	resultCh := make(chan result)

	stats := &divyield.Stats{}

	errs := make([]error, 0)

	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for res := range resultCh {
			if res.Err != nil {
				se := &StatsError{
					Symbol: res.Symbol,
					Err:    res.Err,
				}
				errs = append(errs, se)
			} else {
				stats.Rows = append(stats.Rows, res.Row)
			}
		}

		sort.SliceStable(
			stats.Rows,
			func(i, j int) bool {
				return stats.Rows[i].Profile.Symbol < stats.Rows[j].Profile.Symbol
			},
		)

	}()

LOOP:
	for _, symbol := range symbols {
		symbol := symbol

		select {
		case <-ctx.Done():
			break LOOP
		default:
			// noop
		}

		workerWg.Add(1)
		go func(symbol string) {
			defer workerWg.Done()
			row, err := g.generateStatsRow(ctx, symbol)
			resultCh <- result{Symbol: symbol, Row: row, Err: err}
		}(symbol)
	}

	workerWg.Wait()
	close(resultCh)
	resultWg.Wait()

	if len(errs) > 0 {
		return nil, errs[0]
	}

	g.filter(
		stats,
		g.filterDivYieldFwdSP500MinMax,
		g.filterDivYieldTotalMin,
		g.filterDGRAvgMin,
		g.filterGGRMinMax,
		g.filterNoCutDividend,
		g.filterNoDecliningDGR,
		g.filterDGRYearly,
	)

	return stats, nil
}

type result struct {
	Symbol string
	Row    *divyield.StatsRow
	Err    error
}

func (g *statsGenerator) generateStatsRow(
	ctx context.Context,
	symbol string,
) (*divyield.StatsRow, error) {

	proOut, err := g.db.Profiles(
		ctx,
		&divyield.DBProfilesInput{
			Symbols: []string{symbol},
		},
	)
	if err != nil {
		return nil, err
	}
	profile := proOut.Profiles[0]

	dyf := &divyield.DividendYieldFilter{
		Limit: 1,
	}
	dividendYields, err := g.db.DividendYields(ctx, symbol, dyf)
	if err != nil {
		return nil, fmt.Errorf("get dividend yields: %s", err)
	}

	divYieldFwd := float64(0)
	divFwd := float64(0)
	ggr := float64(0)
	if len(dividendYields) > 0 {
		divYieldFwd = dividendYields[0].ForwardTTM()
		divFwd = dividendYields[0].DividendForwardTTM()
	}
	if g.ggrROI > 0 {
		ggr = g.ggrROI - divYieldFwd
	}

	df := &divyield.DividendFilter{
		From: time.Date(
			time.Now().UTC().Year()-11, time.January, 1,
			0, 0, 0, 0, time.UTC),
		CashOnly: true,
		Regular:  true,
	}
	dividendsDB, err := g.db.Dividends(ctx, symbol, df)
	if err != nil {
		return nil, fmt.Errorf("get dividends: %s", err)
	}

	dividends := make([]*divyield.DividendChange, 0, len(dividendsDB))
	for _, d := range dividendsDB {
		dividends = append(dividends, &divyield.DividendChange{
			Dividend: d,
		})
	}
	g.calcDividendChanges(dividends)

	divChangeMR, divChangeMRDate := g.dividendChangeMR(dividends)

	row := &divyield.StatsRow{
		Profile:              profile,
		DivYieldFwd:          divYieldFwd,
		DivFwd:               divFwd,
		GordonGrowthRate:     ggr,
		Dividends:            dividends,
		DividendChangeMR:     divChangeMR,
		DividendChangeMRDate: divChangeMRDate,
		DGRs:                 g.dgrs(dividends),
		//        DGRs: map[int]float64{
		//			1: g.dgr(dividends, 1),
		//			2: g.dgr(dividends, 2),
		//			3: g.dgr(dividends, 3),
		//			4: g.dgr(dividends, 4),
		//			5: g.dgr(dividends, 5),
		//		},
	}

	return row, nil
}

func (g *statsGenerator) calcDividendChanges(
	dividends []*divyield.DividendChange,
) {
	for i := 0; i <= len(dividends)-2; i++ {
		a0 := dividends[i]
		a0.Change = 0 //math.NaN()
		a1 := dividends[i+1]
		a1.Change = 0 //math.NaN()

		if a0.Currency == a1.Currency {
			a0.Change = ((a0.AmountAdj / a1.AmountAdj) - 1) * 100
		}
	}
}

func (g *statsGenerator) filter(
	stats *divyield.Stats,
	filters ...filterFunc,
) {
	filtered := make([]*divyield.StatsRow, 0, len(stats.Rows))

LOOP_ROWS:
	for _, row := range stats.Rows {
		for _, fn := range filters {
			if ok := fn(row); !ok {
				continue LOOP_ROWS
			}
		}
		filtered = append(filtered, row)
	}
	stats.Rows = filtered
}

type filterFunc func(row *divyield.StatsRow) bool

func (g *statsGenerator) filterNoCutDividend(
	row *divyield.StatsRow,
) bool {
	if !g.noCutDividend {
		return true
	}

	for i := 0; i <= len(row.Dividends)-2; i++ {
		d0 := row.Dividends[i]
		if d0.Change < 0 {
			return false
		}
	}
	return true
}

func (g *statsGenerator) filterDivYieldFwdSP500MinMax(
	row *divyield.StatsRow,
) bool {
	min := g.divYieldFwdMin()
	max := g.divYieldFwdMax()

	if min <= 0 && max <= 0 {
		return true
	}

	v := row.DivYieldFwd

	if min > 0 && (isNaN(v) || v < min) {
		return false
	}

	if max > 0 && (isNaN(v) || max < v) {
		return false
	}

	return true
}

func (g *statsGenerator) filterDivYieldTotalMin(
	row *divyield.StatsRow,
) bool {
	min := g.divYieldTotalMin
	if min <= 0 {
		return true
	}

	return min <= row.DivYieldFwd+row.DGRs[4]
}

func (g *statsGenerator) filterDGRAvgMin(
	row *divyield.StatsRow,
) bool {
	if g.dgrAvgMin <= 0 {
		return true
	}

	return g.dgrAvgMin <= row.DGRs[4]
}

func (g *statsGenerator) filterGGRMinMax(
	row *divyield.StatsRow,
) bool {
	min := g.ggrMin
	max := g.ggrMax

	if min <= 0 && max <= 0 {
		return true
	}

	v := row.GordonGrowthRate

	if min > 0 && (isNaN(v) || v < min) {
		return false
	}

	if max > 0 && (isNaN(v) || max < v) {
		return false
	}

	return true
}

func (g *statsGenerator) filterNoDecliningDGR(
	row *divyield.StatsRow,
) bool {
	if !g.noDecliningDGR {
		return true
	}

	dgrs := []float64{
		//row.DGRs[5],
		row.DGRs[4],
		row.DGRs[3],
		row.DGRs[2],
		row.DGRs[1],
		row.DividendChangeMR,
	}

	dgrsPos := make([]float64, 0, len(dgrs))
	for _, v := range dgrs {
		if v > 0 {
			dgrsPos = append(dgrsPos, v)
		}
	}

	for i := 0; i <= len(dgrsPos)-2; i++ {
		v0 := dgrsPos[i]
		v1 := dgrsPos[i+1]

		if v0 < v1 {
			return true
		}
	}

	return false
}

func (g *statsGenerator) filterDGRYearly(
	row *divyield.StatsRow,
) bool {
	if !g.dgrYearly {
		return true
	}

	m := make(map[int]*divyield.DividendChange)
	endYear := time.Now().UTC().Year() - 1
	startYear := g.startDate.Year() + 1

	for _, v := range row.Dividends {
		y := v.Dividend.ExDate.Year()
		if 0 < v.Change &&
			startYear <= y &&
			y <= endYear {
			m[y] = v
		}
	}

	for i := startYear; i <= endYear; i++ {
		if m[i] == nil {
			return false
		}
	}

	return true
}

func (g *statsGenerator) writef(
	format string,
	v ...interface{},
) {
	if g.writer != nil {
		fmt.Fprintf(g.writer, format, v...)
	}
}

func (g *statsGenerator) dgrs(
	dividends []*divyield.DividendChange,
) map[int]float64 {
	if len(dividends) == 0 {
		return nil
	}

	amounts := make(map[int]float64)
	for _, v := range dividends {
		y := v.ExDate.Year()
		amounts[y] += v.AmountAdj
	}
	//fmt.Println(amounts)

	y := time.Now().UTC().Year()
	ye := y - 1
	changes := make(map[int]float64)
	for _, i := range []int{1, 2, 3, 4} {
		c := math.Pow(
			amounts[ye]/amounts[ye-i],
			float64(1)/float64(i),
		) - 1
		changes[i] = c * 100
	}
	//fmt.Println(changes)

	return map[int]float64{
		1: changes[1],
		2: changes[2],
		3: changes[3],
		4: changes[4],
	}
}

func (g *statsGenerator) dgr(
	dividends []*divyield.DividendChange,
	n int,
) float64 {
	if n < 1 {
		panic("n must be greater than 1")
	}

	if len(dividends) == 0 {
		return 0
	}

	y := time.Now().UTC().Year()
	ed := time.Date(
		y-1, time.December, 31,
		0, 0, 0, 0, time.UTC,
	)
	sd := time.Date(
		y-n, time.January, 1,
		0, 0, 0, 0, time.UTC,
	)

	//changes := make([]float64, 0, n)
	sum := float64(0)
	c := 0
	for _, v := range dividends {
		inPeriod := sd.Unix() < v.ExDate.Unix() &&
			v.ExDate.Unix() < ed.Unix()

		if v.Change != 0 && inPeriod {
			// avg
			sum += v.Change
			c += 1

			// median
			//changes = append(changes, v.Change)
		}
	}

	if c == 0 {
		return 0
	}
	return sum / float64(c)

	//	dgr := float64(0)
	//	if 0 < len(changes) {
	//		sort.Float64s(changes)
	//
	//		//dgr = sum / float64(c)
	//
	//		if len(changes)%2 == 1 {
	//			dgr = changes[(len(changes) / 2)]
	//		} else {
	//			vl := changes[len(changes)/2-1]
	//			vr := changes[len(changes)/2]
	//			dgr = (vl + vr) / 2.0
	//		}
	//	}
	//
	//	return dgr
}

func (g *statsGenerator) dividendChangeMR(
	dividends []*divyield.DividendChange,
) (float64, time.Time) {
	for _, v := range dividends {
		if 0 < v.Change {
			return v.Change, v.ExDate
		}
	}
	return float64(0), time.Time{}
}

type StatsError struct {
	Symbol string
	Err    error
}

func (e *StatsError) Error() string {
	return fmt.Sprintf("%s: %s", e.Symbol, e.Err)
}

type chartGenerator struct {
	writer    io.Writer
	db        divyield.DB
	startDate time.Time
	dir       string
}

func (g *chartGenerator) Generate(
	ctx context.Context,
	stats *divyield.Stats,
) error {
	for _, row := range stats.Rows {
		symbol := row.Profile.Symbol
		dividends := row.Dividends

		yields, err := g.db.DividendYields(
			ctx,
			symbol,
			&divyield.DividendYieldFilter{
				From: g.startDate,
			},
		)

		chartDir := filepath.Join(g.dir, "work/chart")
		err = g.writeFileYields(symbol, yields, chartDir)
		if err != nil {
			return err
		}
		err = g.writeFileDividends(symbol, dividends, chartDir)
		if err != nil {
			return err
		}

		minPrice, maxPrice := g.rangePrices(yields)
		minYieldFwd, maxYieldFwd := g.rangeYieldsFwd(yields)
		yieldStart := yields[0].ForwardTTM()

		//		minYieldTrail, maxYieldTrail := g.rangeYieldsTrail(yields)
		_, maxDiv := g.rangeDividends(dividends)
		minDGR, maxDGR := g.rangeDividendChanges(dividends)

		chartParams := chartParams{
			Yieldsfile: path.Join(
				chartDir,
				symbol+".yields.csv",
			),
			Dividendsfile: path.Join(
				chartDir,
				symbol+".dividends.csv",
			),

			Imgfile: path.Join(
				chartDir,
				symbol+".png",
			),

			XRangeMin: yields[len(yields)-1].
				Date.Format("2006-01-02"),
			XRangeMax: yields[0].
				Date.Format("2006-01-02"),

			TitlePrices:        symbol + " - " + row.Profile.Name + " prices",
			TitleDivYieldFwd:   symbol + " - " + row.Profile.Name + " forward dividend yields",
			TitleDivYieldTrail: symbol + " - " + row.Profile.Name + " trailing dividend yields",
			TitleDividends:     symbol + " - " + row.Profile.Name + " dividends",
			TitleDGR:           symbol + " - " + row.Profile.Name + " dividend growth rates",

			PriceYrMin: math.Max(
				minPrice-((maxPrice-minPrice)*0.1),
				0,
			),
			PriceYrMax: math.Max(
				maxPrice+((maxPrice-minPrice)*0.1),
				0.01,
			),

			YieldFwdYrMin: math.Max(
				minYieldFwd-((maxYieldFwd-minYieldFwd)*0.1),
				0,
			),
			YieldFwdYrMax: math.Max(
				maxYieldFwd+((maxYieldFwd-minYieldFwd)*0.1),
				0.01,
			),
			YieldStart: yieldStart,

			//			YieldTrailYrMin: math.Max(
			//				minYieldTrail-((maxYieldTrail-minYieldTrail)*0.1),
			//				0,
			//			),
			//			YieldTrailYrMax: math.Max(
			//				maxYieldTrail+((maxYieldTrail-minYieldTrail)*0.1),
			//				0.01,
			//			),

			DivYrMin: 0,
			//math.Max(
			//	minDiv-((maxDiv-minDiv)*0.1),
			//	0,
			//),
			DivYrMax: maxDiv * 1.1,
			//math.Max(
			//	maxDiv+((maxDiv-minDiv)*0.1),
			//	0.01,
			//),

			DGRYrMin: minDGR - ((maxDGR - minDGR) * 0.1),
			DGRYrMax: math.Max(
				maxDGR+((maxDGR-minDGR)*0.1),
				0.01,
			),
			DGRAvg: row.DGRs[4],
		}
		chartTmpl, err := template.
			New("plot").
			Parse(chartTmpl)
		if err != nil {
			return err
		}

		plotCommands := bytes.NewBufferString("")
		err = chartTmpl.Execute(
			plotCommands,
			chartParams,
		)
		if err != nil {
			return err
		}

		//fmt.Println(plotCommands)

		plotCommandsStr := nlRE.ReplaceAllString(
			plotCommands.String(),
			" ",
		)

		//fmt.Println("gnuplot -e ", "\""+plotCommandsStr+"\"")
		err = exec.CommandContext(
			ctx,
			"gnuplot", "-e",
			plotCommandsStr,
		).Run()
		if err != nil {
			return fmt.Errorf("%v: %v", symbol, err)
		}

		//g.writef("%s: %s", symbol, "OK")
	}
	return nil
}

func (g *chartGenerator) writeFileYields(
	symbol string,
	yields []*divyield.DividendYield,
	dir string,
) error {
	err := os.MkdirAll(dir, 0666)
	if err != nil {
		return fmt.Errorf("create: %s", err)
	}

	p := filepath.Join(dir, symbol+".yields.csv")
	d, err := os.Create(p)
	if err != nil {
		return fmt.Errorf("create: %s: %s", p, err)
	}
	defer d.Close()

	w := bufio.NewWriter(d)

	_, err = w.Write([]byte("" +
		"Date," +
		"CloseAdjSplits," +
		"DivYieldForwardTTM,",
	))
	if err != nil {
		return err
	}

	for i := 0; i < len(yields); i++ {
		y := yields[i]
		_, err = w.Write([]byte("\n"))
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(
			w,
			"%s,%.2f,%.2f",
			y.Date.Format("2006-01-02"),
			y.CloseAdjSplits,
			y.ForwardTTM(),
		)
		if err != nil {
			return err
		}
	}

	return w.Flush()
}

func (g *chartGenerator) writeFileDividends(
	symbol string,
	dividends []*divyield.DividendChange,
	dir string,
) error {
	p := filepath.Join(dir, symbol+".dividends.csv")
	d, err := os.Create(p)
	if err != nil {
		return fmt.Errorf("create: %s: %s", p, err)
	}
	defer d.Close()

	w := bufio.NewWriter(d)

	_, err = w.Write([]byte("" +
		"Date," +
		"DivAdj," +
		"DGR,",
	))
	if err != nil {
		return err
	}

	for i := 0; i < len(dividends); i++ {
		y := dividends[i]
		_, err = w.Write([]byte("\n"))
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(
			w,
			"%s,%.2f,%.2f",
			y.ExDate.Format("2006-01-02"),
			y.AmountAdj,
			y.Change,
		)
		if err != nil {
			return err
		}
	}

	return w.Flush()
}

func (g *chartGenerator) writef(
	format string,
	v ...interface{},
) {
	if g.writer != nil {
		fmt.Fprintf(g.writer, format, v...)
	}
}

func (g *chartGenerator) rangePrices(
	yields []*divyield.DividendYield,
) (float64, float64) {
	if len(yields) == 0 {
		return 0, 0
	}
	min := yields[0].CloseAdjSplits
	max := yields[0].CloseAdjSplits
	for _, v := range yields {
		if v.CloseAdjSplits < min {
			min = v.CloseAdjSplits
		}
		if v.CloseAdjSplits > max {
			max = v.CloseAdjSplits
		}
	}
	return min, max
}

func (g *chartGenerator) rangeYieldsFwd(
	yields []*divyield.DividendYield,
) (float64, float64) {
	if len(yields) == 0 {
		return 0, 0
	}
	min := yields[0].ForwardTTM()
	max := yields[0].ForwardTTM()
	for _, v := range yields {
		fwd := v.ForwardTTM()
		if fwd < min {
			min = fwd
		}
		if fwd > max {
			max = fwd
		}
	}
	return min, max
}

func (g *chartGenerator) rangeYieldsTrail(
	yields []*divyield.DividendYield,
) (float64, float64) {
	if len(yields) == 0 {
		return 0, 0
	}
	min := yields[0].TrailingTTM()
	max := yields[0].TrailingTTM()
	for _, v := range yields {
		y := v.TrailingTTM()
		if y < min {
			min = y
		}
		if y > max {
			max = y
		}
	}
	return min, max
}

func (g *chartGenerator) rangeDividends(
	a []*divyield.DividendChange,
) (float64, float64) {
	if len(a) == 0 {
		return 0, 0
	}
	min := a[0].AmountAdj
	max := a[0].AmountAdj
	for _, v := range a {
		if v.AmountAdj < min {
			min = v.AmountAdj
		}
		if v.AmountAdj > max {
			max = v.AmountAdj
		}
	}
	return min, max
}

func (g *chartGenerator) rangeDividendChanges(
	a []*divyield.DividendChange,
) (float64, float64) {
	if len(a) == 0 {
		return 0, 0
	}
	min := a[0].Change
	max := a[0].Change
	for _, v := range a {
		if v.Change < min {
			min = v.Change
		}
		if v.Change > max {
			max = v.Change
		}
	}
	return min, max
}

var nlRE = regexp.MustCompile(`\r?\n`)

type chartParams struct {
	Yieldsfile    string
	Dividendsfile string
	Imgfile       string

	XRangeMin string
	XRangeMax string

	TitlePrices        string
	TitleDivYieldFwd   string
	TitleDivYieldTrail string
	TitleDividends     string
	TitleDGR           string

	PriceYrMin float64
	PriceYrMax float64

	YieldFwdYrMin float64
	YieldFwdYrMax float64
	YieldStart    float64

	YieldTrailYrMin float64
	YieldTrailYrMax float64

	DivYrMin float64
	DivYrMax float64

	DGRYrMin float64
	DGRYrMax float64
	DGRAvg   float64
}

const chartTmpl = `
yieldsfile='{{.Yieldsfile}}';
dividendsfile='{{.Dividendsfile}}';
imgfile='{{.Imgfile}}';

set terminal png size 1920,1080;
set output imgfile;

set lmargin  9;
set rmargin  2;

set grid;
set autoscale;
set key outside;
set key bottom right;
set key autotitle columnhead;

set datafile separator ',';

set xdata time;
set timefmt '%Y-%m-%d';
set xrange ['{{.XRangeMin}}':'{{.XRangeMax}}'];
set format x '%Y %b %d';

set multiplot;
set y2tics;
set size 0.96, 0.25;
set style fill solid 1.0;

set origin 0.0,0.75;
set title '{{.TitlePrices}}';
set yrange [{{.PriceYrMin}}:{{.PriceYrMax}}];
set y2range [{{.PriceYrMin}}:{{.PriceYrMax}}];
plot yieldsfile using 1:2 with filledcurves above y = 0 lc 'royalblue';

set origin 0.0,0.50;
set title '{{.TitleDivYieldFwd}}';
set yrange [{{.YieldFwdYrMin}}:{{.YieldFwdYrMax}}];
set y2range [{{.YieldFwdYrMin}}:{{.YieldFwdYrMax}}];
plot yieldsfile using 1:3 with filledcurves above y = 0 lc 'royalblue', {{.YieldStart}} title '' lw 4 lc 'red';

set boxwidth 1 absolute;

set origin 0.0,0.25;
set title '{{.TitleDividends}}';
set yrange [{{.DivYrMin}}:{{.DivYrMax}}];
set y2range [{{.DivYrMin}}:{{.DivYrMax}}];
plot dividendsfile using 1:($2 == 0 ? NaN : $2) with fsteps lw 4 lc 'royalblue', dividendsfile using 1:($2 == 0 ? NaN : $2) with boxes lw 4 lc 'royalblue';

set origin 0.0,0.0;
set title '{{.TitleDGR}}';
set yrange [{{.DGRYrMin}}:{{.DGRYrMax}}];
set y2range [{{.DGRYrMin}}:{{.DGRYrMax}}];
plot dividendsfile using 1:($3 == 0 ? NaN : $3) with boxes lw 4 lc 'royalblue', 0 title '' lw 4 lc 'royalblue', {{.DGRAvg}} title 'DGRAvg' lw 4 lc 'red';

unset multiplot;
`

func isNaN(v float64) bool {
	return math.IsNaN(v) || math.IsInf(v, 1) || math.IsInf(v, -1)
}

var defaultOptions = options{
	writer: nil,
}

type options struct {
	db                divyield.DB
	writer            io.Writer
	dir               string
	dryRun            bool
	startDate         time.Time
	reset             bool
	profileService    divyield.ProfileService
	isinService       divyield.ISINService
	exchangeService   divyield.ExchangeService
	splitService      divyield.SplitService
	dividendService   divyield.DividendService
	priceService      divyield.PriceService
	currencyService   divyield.CurrencyService
	inflationService  divyield.InflationService
	sp500Service      divyield.SP500Service
	financialsService divyield.FinancialsService

	divYieldFwdSP500Min float64
	divYieldFwdSP500Max float64
	divYieldTotalMin    float64
	ggrROI              float64
	ggrMin              float64
	ggrMax              float64
	noCutDividend       bool
	noDecliningDGR      bool
	dgrAvgMin           float64
	dgrYearly           bool
	chart               bool
	force               bool
}

type Option func(o options) options

func Writer(v io.Writer) Option {
	return func(o options) options {
		o.writer = v
		return o
	}
}

func Dir(v string) Option {
	return func(o options) options {
		o.dir = v
		return o
	}
}

func DryRun(v bool) Option {
	return func(o options) options {
		o.dryRun = v
		return o
	}
}

func StartDate(v time.Time) Option {
	return func(o options) options {
		o.startDate = v
		return o
	}
}

func Reset(v bool) Option {
	return func(o options) options {
		o.reset = v
		return o
	}
}

func ProfileService(v divyield.ProfileService) Option {
	return func(o options) options {
		o.profileService = v
		return o
	}
}

func ISINService(v divyield.ISINService) Option {
	return func(o options) options {
		o.isinService = v
		return o
	}
}

func ExchangeService(v divyield.ExchangeService) Option {
	return func(o options) options {
		o.exchangeService = v
		return o
	}
}

func SplitService(v divyield.SplitService) Option {
	return func(o options) options {
		o.splitService = v
		return o
	}
}

func DividendService(v divyield.DividendService) Option {
	return func(o options) options {
		o.dividendService = v
		return o
	}
}

func PriceService(v divyield.PriceService) Option {
	return func(o options) options {
		o.priceService = v
		return o
	}
}

func CurrencyService(v divyield.CurrencyService) Option {
	return func(o options) options {
		o.currencyService = v
		return o
	}
}

func InflationService(
	v divyield.InflationService,
) Option {
	return func(o options) options {
		o.inflationService = v
		return o
	}
}

func SP500Service(
	v divyield.SP500Service,
) Option {
	return func(o options) options {
		o.sp500Service = v
		return o
	}
}

func FinancialsService(
	v divyield.FinancialsService,
) Option {
	return func(o options) options {
		o.financialsService = v
		return o
	}
}

func DB(db divyield.DB) Option {
	return func(o options) options {
		o.db = db
		return o
	}
}

func DividendYieldForwardSP500Min(v float64) Option {
	return func(o options) options {
		o.divYieldFwdSP500Min = v
		return o
	}
}

func DividendYieldTotalMin(v float64) Option {
	return func(o options) options {
		o.divYieldTotalMin = v
		return o
	}
}

func DividendYieldForwardSP500Max(v float64) Option {
	return func(o options) options {
		o.divYieldFwdSP500Max = v
		return o
	}
}

func GordonROI(v float64) Option {
	return func(o options) options {
		o.ggrROI = v
		return o
	}
}

func GordonGrowthRateMin(v float64) Option {
	return func(o options) options {
		o.ggrMin = v
		return o
	}
}

func GordonGrowthRateMax(v float64) Option {
	return func(o options) options {
		o.ggrMax = v
		return o
	}
}

func NoCutDividend(v bool) Option {
	return func(o options) options {
		o.noCutDividend = v
		return o
	}
}

func NoDecliningDGR(v bool) Option {
	return func(o options) options {
		o.noDecliningDGR = v
		return o
	}
}

func DGRAvgMin(v float64) Option {
	return func(o options) options {
		o.dgrAvgMin = v
		return o
	}
}

func DGRYearly(v bool) Option {
	return func(o options) options {
		o.dgrYearly = v
		return o
	}
}

func Chart(v bool) Option {
	return func(o options) options {
		o.chart = v
		return o
	}
}

func Force(v bool) Option {
	return func(o options) options {
		o.force = v
		return o
	}
}
