package divyield

import (
	"context"
	"time"
)

type CurrencyConverter interface {
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
	Rate float64
}

type Command interface {
	Execute() error
}
