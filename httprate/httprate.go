package httprate

import (
    "net/http"
	"golang.org/x/time/rate"
)

type RLClient struct {
	Client      *http.Client
	Ratelimiter *rate.Limiter
}

func (c *RLClient) Do(req *http.Request) (*http.Response, error) {
	err := c.Ratelimiter.Wait(req.Context())
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}


