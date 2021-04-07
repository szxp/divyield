package divyield

import (
	"context"
)

type ChartGenerator interface {
	Generate(ctx context.Context, tickers []string) error
}
