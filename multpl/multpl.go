package multpl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"szakszon.com/divyield"
)

func NewSP500Service() divyield.SP500Service {
	return &sp500Service{
		mu: &sync.RWMutex{},
	}

}

type sp500Service struct {
	mu                 *sync.RWMutex
	sp500DividendYield divyield.SP500DividendYield
}

func (s *sp500Service) DividendYield(
	ctx context.Context,
	in *divyield.SP500DividendYieldInput,
) (*divyield.SP500DividendYieldOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sp500DividendYield == (divyield.SP500DividendYield{}) {
		dv, err := s.dividendYield(ctx)
		if err != nil {
			return nil, err
		}
		s.sp500DividendYield = *dv
	}

	out := &divyield.SP500DividendYieldOutput{
		SP500DividendYield: s.sp500DividendYield,
	}
	return out, nil
}

func (s *sp500Service) dividendYield(
	ctx context.Context,
) (*divyield.SP500DividendYield, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://www.multpl.com/s-p-500-dividend-yield",
		nil,
	)
	req.Header.Set("User-Agent", userAgent)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || 299 < resp.StatusCode {
		return nil, fmt.Errorf(
			"http error: %d",
			resp.StatusCode,
		)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	body := string(b)
	matches := rateRE.FindStringSubmatch(body)
	rateStr := strings.ReplaceAll(matches[1], ",", ".")
	rateStr = strings.TrimSpace(rateStr)
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil {
		return nil, err
	}

	matches = timestampRE.FindStringSubmatch(body)
	timestamp := strings.TrimSpace(matches[1])

	return &divyield.SP500DividendYield{
		Rate:      rate,
		Timestamp: timestamp,
	}, nil
}

var rateRE = regexp.MustCompile(
	`Current S&P 500 Dividend Yield is ([^\s]+)%`,
)

var timestampRE = regexp.MustCompile(
	`(?s)id="timestamp">([^<>]+)<`,
)

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36 OPR/76.0.4017.123"
