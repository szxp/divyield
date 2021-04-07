package divyield

import (
	"context"
)

type StockFetcher interface {
	Fetch(ctx context.Context, tickers []string)
}
