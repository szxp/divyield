package charter

import (
	"context"
	"time"
	"path/filepath"
	"path"
	"fmt"
	"bufio"
	"os"
	"strconv"
	"sort"
	"io"
	"encoding/csv"
	"text/template"
	"bytes"
	"os/exec"
	"regexp"

	"szakszon.com/divyield/logger"
)

type options struct {
	outputDir string
	stocksDir string
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

func StocksDir(dir string) Option {
	return func(o options) options {
		o.stocksDir = dir
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

func Log(l logger.Logger) Option {
	return func(o options) options {
		o.logger = l
		return o
	}
}

var defaultOptions = options{
	outputDir: "",
	stocksDir: "",
	startDate: time.Date(1900, time.January, 1, 1, 0, 0, 0, time.UTC),
	endDate:   time.Time{},
	logger:    nil,
}

func NewCharter(os ...Option) Charter {
	opts := defaultOptions
	for _, o := range os {
		opts = o(opts)
	}
	return Charter{
		opts: opts,
	}
}

type Charter struct {
	opts options
	errs []error
}

func (f *Charter) Chart(ctx context.Context, tickers []string) error {
	if f.opts.endDate.IsZero() {
		f.opts.endDate = time.Now()
	}

	for _, ticker := range tickers {
		pricesPath := filepath.Join(f.opts.stocksDir, ticker, "prices.csv")
		prices, err := parsePrices(ticker, pricesPath)
		if err != nil {
			return fmt.Errorf("parse prices: %s: %s", pricesPath, err)
		}
	
		dividendsPath := filepath.Join(f.opts.stocksDir, ticker, "dividends.csv")
		dividends, err := parseDividends(dividendsPath)
		if err != nil {
			return fmt.Errorf("parse dividends: %s: %s", dividendsPath, err)
		}
	
		setDividendRecent(prices, dividends)

		
		err = os.MkdirAll(f.opts.outputDir, 0666)
		if err != nil {
			return fmt.Errorf("create dir: %s", err)
		}

		dataPath := filepath.Join(f.opts.outputDir, ticker + ".csv")
		d, err := os.Create(dataPath)
		if err != nil {
			return fmt.Errorf("create data file: %s: %s", dataPath, err)
		}
		defer d.Close()
	
		err = writePrices(d, prices, f.opts.startDate, f.opts.endDate)
		if err != nil {
			return fmt.Errorf("create data file: %s: %s", dataPath, err)
		}

		plotParams := plotParams{
			Datafile: path.Join(f.opts.outputDir, ticker+".csv"),
			Imgfile: path.Join(f.opts.outputDir, ticker+".png"),
			TitlePrices:  ticker+" prices",
			TitleDivYield: ticker+" forward dividend yield",
			TitleDividends: ticker+" dividends",
		
		}
		plotCommandsTmpl, err := template.New("plot").Parse(plotCommandsTmpl)
		if err != nil {
			return err
		}

		plotCommands := bytes.NewBufferString("")
		err = plotCommandsTmpl.Execute(plotCommands, plotParams)
		if err != nil { 
			return err 
		}

		plotCommandsStr := nlRE.ReplaceAllString(plotCommands.String(), " ")
		err = exec.CommandContext(ctx, "gnuplot", "-e", plotCommandsStr).Run()
		if err != nil {
			return err
		}
		
		f.log("%s: %s", ticker, "OK")
	}
	return nil
}

func (f *Charter) Errs() []error {
	return f.errs
}

func (f *Charter) log(format string, v ...interface{}) {
	if f.opts.logger != nil {
		f.opts.logger.Logf(format, v...)
	}
}

func writePrices(out io.Writer, prices []*Price, startDate, endDate time.Time) error {
	w := &writer{W: bufio.NewWriter(out)}

	w.WriteString("Date,Price,DividendRecent,DividendForward12M,DividendYieldForward12M")
	for _, p := range prices {

		if !startDate.IsZero() && p.Date.Unix() < startDate.Unix() {
			continue
		}
		if !endDate.IsZero() && endDate.Unix() <= p.Date.Unix() {
			continue
		}

		w.WriteString("\n")
		w.WriteString(p.String())
	}

	err := w.Flush()
	if err != nil {
		return err
	}
	return err
}

func setDividendRecent(prices []*Price, dividends []*Dividend) {
	for _, p := range prices {
	DIVLOOP:
		for _, d := range dividends {
			if !p.Date.Before(d.Date) {
				p.DividendRecent = d.Dividend
				break DIVLOOP
			}
		}
	}
}

type writer struct {
	W   *bufio.Writer
	Err error
}

func (w *writer) Flush() error {
	if w.Err != nil {
		return w.Err
	}
	return w.W.Flush()
}

func (w *writer) WriteString(s string) error {
	if w.Err != nil {
		return w.Err
	}

	_, err := w.W.Write([]byte(s))
	if err != nil {
		w.Err = err
		return err
	}
	return err
}

type Price struct {
	Date           time.Time
	Price          float64
	DividendRecent float64
	PayoutPerYear  int
}

func (r *Price) String() string {
	divForward12M := r.DividendRecent * float64(r.PayoutPerYear)
	divYield := float64(0)
	if r.Price > 0 {
		divYield = (divForward12M / r.Price) * float64(100)
	}

	return fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f",
		r.Date.Format("2006-01-02"),
		r.Price,
		r.DividendRecent,
		divForward12M,
		divYield,
	)
}

func parsePrices(ticker, p string) ([]*Price, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %s", err)
	}

	records := make([]*Price, 0)

	for _, row := range rows[1:] {
		price := float64(0)

		date, err := time.Parse("2006-01-02", row[0])
		if err != nil {
			return nil, err
		}

		if row[5] != "null" {
			price, err = strconv.ParseFloat(row[5], 64)
			if err != nil {
				return nil, err
			}
		}

		record := &Price{Date: date, Price: price, PayoutPerYear: payoutPerYear[ticker]}
		records = append(records, record)
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Date.After(records[j].Date)
	})

	return records, nil
}

type Dividend struct {
	Date     time.Time
	Dividend float64
}

func (r *Dividend) String() string {
	return fmt.Sprintf("%s,%s",
		r.Date.Format("2006-01-02"),
		strconv.FormatFloat(r.Dividend, 'f', -1, 64),
	)
}

func parseDividends(p string) ([]*Dividend, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %s", err)
	}

	records := make([]*Dividend, 0)

	for _, row := range rows[1:] {
		div := float64(0)

		date, err := time.Parse("2006-01-02", row[0])
		if err != nil {
			return nil, err
		}

		if row[1] != "null" {
			div, err = strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, err
			}
		}

		record := &Dividend{Date: date, Dividend: div}
		records = append(records, record)
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Date.After(records[j].Date)
	})

	return records, nil
}

var nlRE = regexp.MustCompile(`\r?\n`)

type plotParams struct {
	Datafile string
	Imgfile    string
	TitlePrices string
	TitleDivYield string
	TitleDividends string
}

const plotCommandsTmpl = `
datafile='{{.Datafile}}';
imgfile='{{.Imgfile}}';
titleprices='{{.TitlePrices}}';
titledivyield='{{.TitleDivYield}}';
titledividends='{{.TitleDividends}}';

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
set format x '%Y %b %d';

set multiplot;
set size 1, 0.33;

set origin 0.0,0.66;
set title titleprices;
plot datafile using 1:2 with filledcurves above y = 0;

set origin 0.0,0.33;
set title titledivyield;
plot datafile using 1:5 with filledcurves above y = 0;

set origin 0.0,0.0;
set title titledividends;
plot datafile using 1:3 with lines;

unset multiplot;
`

var payoutPerYear map[string]int = map[string]int{
	"ABM":   4,
	"ADM":   4,
	"ADP":   4,
	"AFL":   4,
	"ALB":   4,
	"AOS":   4,
	"APD":   4,
	"AROW":  4,
	"ARTNA": 4,
	"ATO":   4,
	"ATR":   4,
	"AWR":   4,
	"BANF":  4,
	"BDX":   4,
	"BEN":   4,
	"BF-B":  4,
	"BKH":   4,
	"BMI":   4,
	"BRC":   4,
	"BRO":   4,
	"CAT":   4,
	"CB":    4,
	"CBSH":  4,
	"CBU":   4,
	"CFR":   4,
	"CHD":   4,
	"CINF":  4,
	"CL":    4,
	"CLX":   4,
	"CNI":   4,
	"CPKF":  4,
	"CSL":   4,
	"CSVI":  4,
	"CTAS":  4,
	"CTBI":  4,
	"CVX":   4,
	"CWT":   4,
	"DCI":   4,
	"DOV":   4,
	"EBTC":  4,
	"ECL":   4,
	"ED":    4,
	"EFSI":  4,
	"EMR":   4,
	"ENB":   4,
	"ERIE":  4,
	"ESS":   4,
	"EV":    4,
	"EXPD":  2,
	"FELE":  4,
	"FFMR":  4,
	"FLIC":  4,
	"FMCB":  2,
	"FRT":   4,
	"FUL":   4,
	"GD":    4,
	"GPC":   4,
	"GRC":   4,
	"GWW":   4,
	"HRL":   4,
	"IBM":   4,
	"ITW":   4,
	"JKHY":  4,
	"JNJ":   4,
	"JW-A":  4,
	"KMB":   4,
	"KO":    4,
	"LANC":  4,
	"LECO":  4,
	"LEG":   4,
	"LIN":   4,
	"LOW":   4,
	"MATW":  4,
	"MCD":   4,
	"MCY":   4,
	"MDT":   4,
	"MDU":   4,
	"MGEE":  4,
	"MGRC":  4,
	"MKC":   4,
	"MMM":   4,
	"MO":    4,
	"MSA":   4,
	"MSEX":  4,
	"NC":    4,
	"NDSN":  4,
	"NEE":   4,
	"NFG":   4,
	"NIDB":  4,
	"NJR":   4,
	"NNN":   4,
	"NUE":   4,
	"NWN":   4,
	"O":     12,
	"ORI":   4,
	"OZK":   4,
	"PBCT":  4,
	"PEP":   4,
	"PG":    4,
	"PH":    4,
	"PII":   4,
	"PNR":   4,
	"PPG":   4,
	"PSBQ":  2,
	"RLI":   4,
	"RNR":   4,
	"ROP":   4,
	"RPM":   4,
	"SBSI":  4,
	"SCL":   4,
	"SEIC":  2,
	"SHW":   4,
	"SJW":   4,
	"SON":   4,
	"SPGI":  4,
	"SRCE":  4,
	"SWK":   4,
	"SYK":   4,
	"SYY":   4,
	"T":     4,
	"TDS":   4,
	"TGT":   4,
	"THFF":  2,
	"TMP":   4,
	"TNC":   4,
	"TR":    4,
	"TRI":   4,
	"TROW":  4,
	"TYCB":  4,
	"UBSI":  4,
	"UGI":   4,
	"UHT":   4,
	"UMBF":  4,
	"UVV":   4,
	"VFC":   4,
	"WABC":  4,
	"WBA":   4,
	"WEYS":  4,
	"WMT":   4,
	"WST":   4,
	"WTRG":  4,
	"XOM":   4,
	"A":     4,
	"AAN":   4,
	"ABC":   4,
	"ABR":   4,
	"ACN":   4,
	"ADI":   4,
	"AEL":   1,
	"AEP":   4,
	"AES":   4,
	"AFG":   4,
	"AGM":   4,
	"AGO":   4,
	"AIRC":  4,
	"AIT":   4,
	"AIZ":   4,
	"AJG":   4,
	"ALE":   4,
	"ALL":   4,
	"ALRS":  4,
	"AMGN":  4,
	"AMP":   4,
	"AMT":   4,
	"ANDE":  4,
	"ANTM":  4,
	"APLO":  4,
	"APOG":  4,
	"AQN":   4,
	"ARE":   4,
	"ASH":   4,
	"ATLO":  4,
	"ATRI":  4,
	"ATVI":  1,
	"AUB":   4,
	"AUBN":  4,
	"AVA":   4,
	"AVGO":  4,
	"AVNT":  4,
	"AVY":   4,
	"AWK":   4,
	"AXS":   4,
	"BAH":   4,
	"BAM":   4,
	"BBY":   4,
	"BCPC":  1,
	"BEP":   4,
	"BHB":   4,
	"BIP":   4,
	"BK":    4,
	"BKSC":  4,
	"BKUTK": 2,
	"BLK":   4,
	"BMRC":  4,
	"BMTC":  4,
	"BMY":   4,
	"BOKF":  4,
	"BR":    4,
	"CAH":   4,
	"CASS":  4,
	"CASY":  4,
	"CATC":  4,
	"CBOE":  4,
	"CCFN":  4,
	"CCOI":  4,
	"CE":    4,
	"CHDN":  1,
	"CHE":   4,
	"CHRW":  4,
	"CIVB":  4,
	"CMA":   4,
	"CMCSA": 4,
	"CME":   4,
	"CMI":   4,
	"CMS":   4,
	"CNS":   4,
	"COR":   4,
	"CORE":  4,
	"COST":  4,
	"CPK":   4,
	"CPT":   4,
	"CSCO":  4,
	"CSX":   4,
	"CUBE":  4,
	"CULP":  4,
	"CZFS":  4,
	"DDS":   4,
	"DEI":   4,
	"DFS":   4,
	"DGICA": 4,
	"DGICB": 4,
	"DGX":   4,
	"DLR":   4,
	"DOX":   4,
	"DTE":   4,
	"DUK":   4,
	"EBMT":  4,
	"EIX":   4,
	"ELS":   4,
	"EMCF":  4,
	"EMN":   4,
	"ENSG":  4,
	"EPD":   4,
	"ES":    4,
	"ETN":   4,
	"EVBN":  2,
	"EVR":   4,
	"EVRG":  4,
	"EXR":   4,
	"FAF":   4,
	"FAST":  4,
	"FDS":   4,
	"FFG":   4,
	"FFIN":  4,
	"FIBK":  4,
	"FISI":  4,
	"FITB":  4,
	"FLO":   4,
	"FMAO":  4,
	"FMBH":  2,
	"FNV":   4,
	"GATX":  4,
	"GFF":   4,
	"GGG":   4,
	"GL":    4,
	"GLW":   4,
	"GNTX":  4,
	"GS":    4,
	"HAS":   4,
	"HBAN":  4,
	"HBNC":  4,
	"HCSG":  4,
	"HD":    4,
	"HEI":   2,
	"HFWA":  4,
	"HI":    4,
	"HIFS":  4,
	"HIG":   4,
	"HMN":   4,
	"HNI":   4,
	"HOMB":  4,
	"HON":   4,
	"HONT":  4,
	"HPQ":   4,
	"HRC":   4,
	"HSY":   4,
	"HUBB":  4,
	"HUM":   4,
	"HWKN":  4,
	"IBOC":  2,
	"IEX":   4,
	"IFF":   4,
	"INDB":  4,
	"INGR":  4,
	"INTU":  4,
	"IP":    4,
	"IRM":   4,
	"ISBA":  4,
	"ISBC":  4,
	"JBHT":  4,
	"JJSF":  4,
	"JPM":   4,
	"K":     4,
	"KALU":  4,
	"KEY":   4,
	"KLAC":  4,
	"KR":    4,
	"KW":    4,
	"KWR":   4,
	"LAD":   4,
	"LARK":  4,
	"LAZ":   4,
	"LBAI":  4,
	"LFUS":  4,
	"LHX":   4,
	"LII":   4,
	"LKFN":  4,
	"LMAT":  4,
	"LMT":   4,
	"LNC":   4,
	"LNN":   4,
	"LNT":   4,
	"LSTR":  4,
	"LYB":   4,
	"LYBC":  4,
	"MA":    4,
	"MAA":   4,
	"MAIN":  12,
	"MAN":   2,
	"MBWM":  4,
	"MCHP":  4,
	"MCK":   4,
	"MCO":   4,
	"MGA":   4,
	"MKTX":  4,
	"MMC":   4,
	"MMP":   4,
	"MNRO":  4,
	"MOFG":  4,
	"MORN":  4,
	"MPC":   4,
	"MRK":   4,
	"MSFT":  4,
	"MSI":   4,
	"MSM":   4,
	"MYBF":  4,
	"NEU":   4,
	"NHC":   4,
	"NHI":   4,
	"NI":    4,
	"NKE":   4,
	"NOC":   4,
	"NP":    4,
	"NRIM":  4,
	"NSP":   4,
	"NUS":   4,
	"NWBI":  4,
	"NWE":   4,
	"NWFL":  4,
	"ODC":   4,
	"OGE":   4,
	"OHI":   4,
	"OKE":   4,
	"OMC":   4,
	"ORCL":  4,
	"PAYX":  4,
	"PB":    4,
	"PEG":   4,
	"PETS":  4,
	"PFC":   4,
	"PFE":   4,
	"PFG":   4,
	"PLOW":  4,
	"PM":    4,
	"PNBI":  4,
	"PNC":   4,
	"PNM":   4,
	"POOL":  4,
	"POR":   4,
	"PPL":   4,
	"PPLL":  4,
	"PRGO":  4,
	"PRI":   4,
	"PRU":   4,
	"QCOM":  4,
	"QNBC":  4,
	"QNTO":  4,
	"R":     4,
	"RBA":   4,
	"RBC":   4,
	"RBCAA": 4,
	"RGA":   4,
	"RGCO":  4,
	"RGLD":  4,
	"RGP":   4,
	"RHI":   4,
	"ROK":   4,
	"RS":    4,
	"RSG":   4,
	"SASR":  4,
	"SBUX":  4,
	"SCI":   4,
	"SFNC":  4,
	"SJI":   4,
	"SJM":   4,
	"SLG":   12,
	"SLGN":  4,
	"SMBC":  4,
	"SMG":   4,
	"SNA":   4,
	"SO":    4,
	"SPTN":  4,
	"SR":    4,
	"SRE":   4,
	"STAG":  12,
	"STE":   4,
	"STLD":  4,
	"STT":   4,
	"SWX":   4,
	"SXI":   4,
	"SXT":   4,
	"SYBT":  4,
	"TBNK":  4,
	"TFC":   4,
	"THG":   4,
	"THO":   4,
	"TPL":   1,
	"TRN":   4,
	"TRNO":  4,
	"TRV":   4,
	"TSCO":  4,
	"TTC":   4,
	"TXN":   4,
	"UDR":   4,
	"UNH":   4,
	"UNM":   4,
	"UNP":   4,
	"UPS":   4,
	"USB":   4,
	"UTMD":  4,
	"V":     4,
	"VLO":   4,
	"VSEC":  4,
	"VZ":    4,
	"WAFD":  4,
	"WASH":  4,
	"WBS":   4,
	"WCN":   4,
	"WDFC":  4,
	"WEC":   4,
	"WHR":   4,
	"WLK":   4,
	"WLTW":  4,
	"WM":    4,
	"WMPN":  1,
	"WOR":   4,
	"WPC":   4,
	"WRB":   4,
	"WSBC":  4,
	"WSM":   4,
	"WTBA":  4,
	"XEL":   4,
	"XLNX":  4,
	"XYL":   4,
	"YORW":  4,
	"AAPL":  4,
	"ABBV":  4,
	"ABT":   4,
	"ACC":   4,
	"ADC":   12,
	"AEE":   4,
	"AEM":   4,
	"AGCO":  4,
	"AL":    4,
	"ALG":   4,
	"ALLE":  4,
	"ALLY":  4,
	"ALTA":  4,
	"AMNB":  4,
	"AMSF":  4,
	"AON":   4,
	"APH":   4,
	"ASB":   4,
	"AVB":   4,
	"AVT":   4,
	"AXP":   4,
	"BAC":   4,
	"BANR":  4,
	"BC":    4,
	"BDL":   1,
	"BORT":  4,
	"BPOP":  4,
	"BPY":   4,
	"BSRR":  4,
	"BUSE":  4,
	"BWFG":  4,
	"BWXT":  4,
	"BXS":   4,
	"C":     4,
	"CABO":  4,
	"CBAN":  4,
	"CBT":   4,
	"CCBG":  4,
	"CCI":   4,
	"CCMP":  4,
	"CDW":   4,
	"CFFI":  4,
	"CFG":   4,
	"CGNX":  4,
	"CHCO":  4,
	"CHCT":  4,
	"CLDB":  4,
	"CNA":   4,
	"CNO":   4,
	"CONE":  4,
	"CPF":   4,
	"CQP":   4,
	"CRAI":  4,
	"CSGS":  4,
	"CSWC":  4,
	"CTO":   4,
	"CTRE":  4,
	"CVBF":  4,
	"CVGW":  1,
	"DG":    4,
	"DHI":   4,
	"DHIL":  1,
	"DHR":   4,
	"DKL":   4,
	"DKS":   4,
	"DLB":   4,
	"DPZ":   4,
	"DRE":   4,
	"EBSB":  4,
	"EFSC":  4,
	"EGP":   4,
	"EHC":   4,
	"EIG":   4,
	"EQIX":  4,
	"ETR":   4,
	"EVA":   4,
	"EXC":   4,
	"EXPO":  4,
	"EXSR":  4,
	"FBHS":  4,
	"FBIZ":  4,
	"FBVA":  4,
	"FCBC":  4,
	"FCCO":  4,
	"FCCY":  4,
	"FCPT":  4,
	"FDBC":  4,
	"FFNW":  4,
	"FHN":   4,
	"FIX":   4,
	"FMBI":  4,
	"FMBM":  4,
	"FMNB":  4,
	"FNCB":  4,
	"FNF":   4,
	"FNLC":  4,
	"FR":    4,
	"FRAF":  4,
	"FRC":   4,
	"FRME":  4,
	"FSBW":  4,
	"FSFG":  4,
	"FSV":   4,
	"FULT":  4,
	"FWRD":  4,
	"FXNC":  4,
	"G":     4,
	"GABC":  4,
	"GAIN":  12,
	"GBCI":  4,
	"GBX":   4,
	"GCBC":  4,
	"GFED":  4,
	"GHC":   4,
	"GILD":  4,
	"GRA":   4,
	"GSBC":  4,
	"GTY":   4,
	"HBCP":  4,
	"HCKT":  4,
	"HESM":  4,
	"HFBL":  4,
	"HII":   4,
	"HLAN":  4,
	"HLI":   4,
	"HNNA":  4,
	"HOFT":  4,
	"HPE":   4,
	"HTA":   4,
	"HTBK":  4,
	"HTH":   4,
	"HTLF":  4,
	"HURC":  4,
	"HWBK":  4,
	"HY":    4,
	"IBCP":  4,
	"IBTX":  4,
	"ICE":   4,
	"IDA":   4,
	"IHC":   2,
	"IIPR":  4,
	"INTC":  4,
	"IOSP":  2,
	"IPG":   4,
	"ISTR":  4,
	"ITT":   4,
	"JEF":   4,
	"JOUT":  4,
	"KAI":   4,
	"KBAL":  4,
	"KNSL":  4,
	"KRC":   4,
	"KRNY":  4,
	"LAND":  12,
	"LCII":  4,
	"LLY":   4,
	"LOGI":  1,
	"LRCX":  4,
	"LW":    4,
	"MAS":   4,
	"MATX":  4,
	"MCBC":  4,
	"MDLZ":  4,
	"MED":   4,
	"MET":   4,
	"MFC":   4,
	"MFNC":  4,
	"MGP":   4,
	"MLM":   4,
	"MNAT":  4,
	"MPB":   4,
	"MPLX":  4,
	"MPW":   4,
	"MS":    4,
	"MSBI":  4,
	"MSCI":  4,
	"MTRN":  4,
	"MVBF":  4,
	"MWA":   4,
	"NATI":  4,
	"NBTB":  4,
	"NDAQ":  4,
	"NEP":   4,
	"NFBK":  4,
	"NNI":   4,
	"NPO":   4,
	"NSA":   4,
	"NSC":   4,
	"NTAP":  4,
	"NTRS":  4,
	"NXRT":  4,
	"NXST":  4,
	"OC":    4,
	"ODFL":  4,
	"OGS":   4,
	"OLED":  4,
	"ORRF":  4,
	"OSK":   4,
	"OTEX":  4,
	"OTTR":  4,
	"OVLY":  2,
	"PEBK":  4,
	"PEBO":  4,
	"PFIS":  4,
	"PKBK":  4,
	"PLD":   4,
	"PNW":   4,
	"POWI":  4,
	"PPBN":  4,
	"PRGS":  4,
	"PSX":   4,
	"PSXP":  4,
	"QSR":   4,
	"QTS":   4,
	"RE":    4,
	"REG":   4,
	"REXR":  4,
	"RF":    4,
	"RJF":   4,
	"RMAX":  4,
	"RMD":   4,
	"RVSB":  4,
	"SAP":   1,
	"SBFG":  4,
	"SCHW":  4,
	"SCVL":  4,
	"SF":    4,
	"SFBC":  4,
	"SFBS":  4,
	"SGU":   4,
	"SHBI":  4,
	"SHEN":  1,
	"SHLX":  4,
	"SIGI":  4,
	"SMMF":  4,
	"SNE":   2,
	"SNV":   4,
	"SOMC":  4,
	"SSB":   4,
	"SSD":   4,
	"STBA":  4,
	"STBI":  4,
	"STOR":  4,
	"STZ":   4,
	"SWKS":  4,
	"SYX":   4,
	"TAIT":  4,
	"TCBK":  4,
	"TCF":   4,
	"TEL":   4,
	"TFSL":  4,
	"TKR":   4,
	"TOWN":  4,
	"TRUX":  4,
	"TSBK":  4,
	"TSN":   4,
	"TTEC":  2,
	"TTEK":  4,
	"TYBT":  2,
	"UBCP":  4,
	"UCBI":  4,
	"UFPI":  4,
	"UNB":   4,
	"UNTY":  4,
	"UTL":   4,
	"VALU":  4,
	"VMC":   4,
	"VVV":   4,
	"WDFN":  4,
	"WLKP":  4,
	"WMS":   4,
	"WSFS":  4,
	"WSO":   4,
	"WTBFA": 4,
	"WTFC":  4,
	"WTS":   4,
	"WU":    4,
	"XRAY":  4,
	"ZION":  4,
	"ZTS":   4,
}


