package httprate

import (
	"golang.org/x/time/rate"
	"net/http"
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
