package mnb

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
    "fmt"
    "strings"

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
		"https://www.mnb.hu/",
		nil,
	)
	req.Header.Set(
		"User-Agent",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36 OPR/76.0.4017.123",
	)
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
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil {
		return nil, err
	}

	matches = periodRE.FindStringSubmatch(body)
	period := matches[1]

	return &divyield.Inflation{
		Rate:   rate,
		Period: period,
	}, nil
}

var rateRE = regexp.MustCompile(
	`(?s)Infláció.*KSH:.*>\s*([^>%\s]+)\s*%`,
)

var periodRE = regexp.MustCompile(
	`(?s)Infláció.*>([^>]+),\s*KSH`,
)
