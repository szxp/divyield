package divyield

import (
	"context"
	"fmt"
	"math"
	"time"
)

type Command interface {
	Execute(ctx context.Context) error
}

type DB interface {
	InitSchema(
		ctx context.Context,
		tickers []string,
	) error

	Prices(
		ctx context.Context,
		ticker string,
		f *PriceFilter,
	) ([]*Price, error)

	SavePrices(
		ctx context.Context,
		in *DBSavePricesInput,
	) (*DBSavePricesOutput, error)

	Dividends(
		ctx context.Context,
		ticker string,
		f *DividendFilter,
	) ([]*Dividend, error)

	SaveDividends(
		ctx context.Context,
		in *DBSaveDividendsInput,
	) (*DBSaveDividendsOutput, error)

	DividendYields(
		ctx context.Context,
		ticker string,
		f *DividendYieldFilter,
	) ([]*DividendYield, error)

	Splits(
		ctx context.Context,
		ticker string,
		f *SplitFilter,
	) ([]*Split, error)

	SaveSplits(
		ctx context.Context,
		in *DBSaveSplitsInput,
	) (*DBSaveSplitsOutput, error)

	SaveProfile(
		ctx context.Context,
		in *DBSaveProfileInput,
	) (*DBSaveProfileOutput, error)

	Profiles(
		ctx context.Context,
		in *DBProfilesInput,
	) (*DBProfilesOutput, error)
}

type DBSavePricesInput struct {
	Symbol string
	Prices []*Price
	Reset  bool
}

type DBSavePricesOutput struct {
}

type DBSaveDividendsInput struct {
	Symbol    string
	Dividends []*Dividend
	Reset     bool
}

type DBSaveDividendsOutput struct {
}

type DBSaveSplitsInput struct {
	Symbol string
	Splits []*Split
	Reset  bool
}

type DBSaveSplitsOutput struct {
}

type DBSaveProfileInput struct {
	Symbol  string
	Profile *Profile
}

type DBSaveProfileOutput struct {
}

type DBProfilesInput struct {
	Symbols []string
}

type DBProfilesOutput struct {
	Profiles []*Profile
}

const DateFormat = "2006-01-02"

type PriceService interface {
	Fetch(
		ctx context.Context,
		in *PriceFetchInput,
	) (*PriceFetchOutput, error)
}

type PriceFetchInput struct {
	Symbol string
	From   time.Time
}

type PriceFetchOutput struct {
	Prices []*Price
}
type Price struct {
	Date           time.Time
	Symbol         string
	Close          float64
	CloseAdjSplits float64
	High           float64
	Low            float64
	Open           float64
	Volume         float64
	Currency       string
}

func (p *Price) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(p.Date).Format(DateFormat),
		p.Close,
	)
}

type PriceFilter struct {
	From  time.Time
	Limit uint64
}

type DividendService interface {
	Fetch(
		ctx context.Context,
		in *DividendFetchInput,
	) (*DividendFetchOutput, error)
}

type DividendFetchInput struct {
	Symbol string
	From   time.Time
}

type DividendFetchOutput struct {
	Dividends []*Dividend
}

type Dividend struct {
	ID          int64
	ExDate      time.Time
	Amount      float64
	AmountAdj   float64
	Currency    string
	Frequency   int
	Symbol      string
	PaymentType string
	Created     time.Time
}

func (d *Dividend) Year() int {
	return d.ExDate.Year()
}

func (d *Dividend) String() string {
	return fmt.Sprintf(
		"Dividend(ID=%v ExDate=%v Amount=%v AmoundAdj=%v Currency=%v)",
		d.ID,
		time.Time(d.ExDate).Format(DateFormat),
		d.Amount,
		d.AmountAdj,
		d.Currency,
	)
}

func (d *Dividend) AmountAdjYear() float64 {
	return d.AmountAdj * float64(d.Frequency)
}

type DividendFilter struct {
	From     time.Time
	Limit    uint64
	CashOnly bool
	Regular  bool
}

type DividendYield struct {
	Date                   time.Time
	Close                  float64
	CloseAdjSplits         float64
	DividendAdj            float64
	Frequency              int
	DividendAdjTrailingTTM float64
}

func (y *DividendYield) DividendForwardTTM() float64 {
	return y.DividendAdj * float64(y.Frequency)
}

func (y *DividendYield) ForwardTTM() float64 {
	if y.CloseAdjSplits == 0 {
		return 0
	}
	return ((y.DividendAdj * float64(y.Frequency)) /
		y.CloseAdjSplits) * 100
}

func (y *DividendYield) TrailingTTM() float64 {
	if y.CloseAdjSplits == 0 {
		return 0
	}
	return (y.DividendAdjTrailingTTM / y.CloseAdjSplits) * 100
}

type DividendYieldFilter struct {
	From  time.Time
	Limit uint64
}

type StockFetcher interface {
	Fetch(ctx context.Context, tickers []string)
}

type SplitService interface {
	Fetch(
		ctx context.Context,
		in *SplitFetchInput,
	) (*SplitFetchOutput, error)
}

type SplitFetchInput struct {
	Symbol string
	From   time.Time
}

type SplitFetchOutput struct {
	Splits []*Split
}

type Split struct {
	ExDate     time.Time
	ToFactor   float64
	FromFactor float64
}

func (s *Split) String() string {
	return fmt.Sprintf("%v: %v",
		time.Time(s.ExDate).Format(DateFormat),
		float64(s.ToFactor)/float64(s.FromFactor),
	)
}

type SplitFilter struct {
	Limit uint64
}

type ProfileService interface {
	Fetch(
		ctx context.Context,
		in *ProfileFetchInput,
	) (*ProfileFetchOutput, error)
}

type ProfileFetchInput struct {
	Symbol string
}

type ProfileFetchOutput struct {
	Profile *Profile
}

type Profile struct {
	Symbol         string
	Name           string
	Exchange       string
	IssueType      string
	Industry       string
	Sector         string
	Description    string
	Website        string
	PrimarySicCode int
	Address        string
	City           string
	Zip            string
	State          string
	Country        string
	Phone          string
	Pulled         time.Time
}

type ISINService interface {
	Resolve(
		ctx context.Context,
		in *ISINResolveInput,
	) (*ISINResolveOutput, error)
}

type ISINResolveInput struct {
	ISIN string
}

type ISINResolveOutput struct {
	Symbols []*SymbolISIN
}

type SymbolISIN struct {
	Symbol   string
	Exchange string
	Region   string
}

type CurrencyService interface {
	Convert(
		ctx context.Context,
		in *CurrencyConvertInput,
	) (*CurrencyConvertOutput, error)
}

type CurrencyConvertInput struct {
	From   string
	To     string
	Amount float64
	Date   time.Time
}

type CurrencyConvertOutput struct {
	Amount float64
	Rate   float64
}

type ExchangeService interface {
	Fetch(
		ctx context.Context,
		in *ExchangeFetchInput,
	) (*ExchangeFetchOutput, error)
}

type ExchangeFetchInput struct {
}

type ExchangeFetchOutput struct {
	Exchanges []*Exchange
}

type Exchange struct {
	Region      string
	Exchange    string
	Suffix      string
	Currency    string
	Description string
}

type Stats struct {
	Rows []*StatsRow
}

type StatsRow struct {
	//	Symbol               string
	Profile              *Profile
	DivYieldFwd          float64
	DivFwd               float64
	GordonGrowthRate     float64
	Dividends            []*DividendChange
	DividendChangeMR     float64
	DividendChangeMRDate time.Time
	DGRs                 map[int]float64
}

type DividendChange struct {
	*Dividend
	Change float64
}

type InflationService interface {
	Fetch(
		ctx context.Context,
		in *InflationFetchInput,
	) (*InflationFetchOutput, error)
}

type InflationFetchInput struct{}

type InflationFetchOutput struct {
	Inflation Inflation
}

type Inflation struct {
	Rate   float64
	Period string
}

type SP500Service interface {
	DividendYield(
		ctx context.Context,
		in *SP500DividendYieldInput,
	) (*SP500DividendYieldOutput, error)
}

type SP500DividendYieldInput struct{}

type SP500DividendYieldOutput struct {
	SP500DividendYield SP500DividendYield
}

type SP500DividendYield struct {
	Rate      float64
	Timestamp string
}

type FinancialsService interface {
	CashFlow(
		ctx context.Context,
		in *FinancialsCashFlowInput,
	) (*FinancialsCashFlowOutput, error)

	Statements(
		ctx context.Context,
		in *FinancialsStatementsInput,
	) (*FinancialsStatementsOutput, error)

	PullValuation(
		ctx context.Context,
		in *FinancialsPullValuationInput,
	) (chan string, chan *FinancialsPullValuationOutput)
}

type FinancialsCashFlowInput struct {
	Symbol string
}

type FinancialsCashFlowOutput struct {
	CashFlow []*FinancialsCashFlow
}

type FinancialsCashFlow struct {
	Period       string
	DividendPaid float64
	FreeCashFlow float64
}

func (f *FinancialsCashFlow) DPSPerFCF() float64 {
	if f.FreeCashFlow == 0 {
		return 0
	}
	return (math.Abs(f.DividendPaid) / f.FreeCashFlow) * 100
}

type FinancialsStatementsInput struct {
	URL string
}

type FinancialsStatementsOutput struct {
	IncomeStatement string
	BalanceSheet    string
	CashFlow        string
}

type FinancialsPullValuationInput struct {
	//URLs []string
}

type FinancialsPullValuationOutput struct {
	URL             string
	Valuation       [][]string
	IncomeStatement string
	BalanceSheet    string
	CashFlow        string
	Err             error
}
