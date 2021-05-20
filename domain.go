package divyield

import (
	"context"
	"time"
)

type Command interface {
	Execute(ctx context.Context) error
}

type CompanyProfileService interface {
	Fetch(
		ctx context.Context,
		in *CompanyProfileFetchInput,
	) (*CompanyProfileFetchOutput, error)
}

type CompanyProfileFetchInput struct {
	Symbol string
}

type CompanyProfileFetchOutput struct {
	CompanyProfile *CompanyProfile
}

type CompanyProfile struct {
	Symbol         string
	Name           string
	Exchange       string
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
