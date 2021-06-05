package mnb

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

func NewInflationService() divyield.InflationService {
	return &inflationService{
		mu: &sync.RWMutex{},
	}

}

type inflationService struct {
	mu        *sync.RWMutex
	inflation divyield.Inflation
}

func (s *inflationService) Fetch(
	ctx context.Context,
	in *divyield.InflationFetchInput,
) (*divyield.InflationFetchOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inflation == (divyield.Inflation{}) {
		inf, err := s.fetch(ctx)
		if err != nil {
			return nil, err
		}
		s.inflation = *inf
	}

	out := &divyield.InflationFetchOutput{
		Inflation: s.inflation,
	}
	return out, nil
}

func (s *inflationService) fetch(
	ctx context.Context,
) (*divyield.Inflation, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://www.mnb.hu/web/fooldal",
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
	period := strings.TrimSpace(matches[1])
	rateStr := strings.ReplaceAll(matches[2], ",", ".")
	rateStr = strings.TrimSpace(rateStr)
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil {
		return nil, err
	}

    return &divyield.Inflation{
		Rate:   rate,
		Period: period,
	}, nil
}

var rateRE = regexp.MustCompile(
    `(?s)Infláció.*>([^<>]+KSH).*-value">([^>]+)<span`,
)

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36 OPR/76.0.4017.123"
