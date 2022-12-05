package server

import (
	"sync"
	"time"
)

type rateLimiter struct {
	sync.RWMutex
	requestsLimit int
	requests      map[string]int

	cleanupInterval time.Duration
}

func newRateLimiter(cleanupInterval time.Duration, requestsLimit int) *rateLimiter {
	limiter := &rateLimiter{
		requests:        map[string]int{},
		cleanupInterval: cleanupInterval,
		requestsLimit:   requestsLimit,
	}
	if cleanupInterval > 0 {
		go limiter.runCleaner()
	}

	return limiter
}

func (r *rateLimiter) canProcess(accountID string) bool {
	r.Lock()
	defer r.Unlock()
	requests, ok := r.requests[accountID]
	if !ok {
		r.requests[accountID] = 1
		return true
	}

	if requests >= r.requestsLimit {
		return false
	}

	r.requests[accountID] = requests + 1
	return true
}

func (r *rateLimiter) runCleaner() {
	ticker := time.NewTicker(r.cleanupInterval).C
	for {
		<-ticker
		r.resetCounters()
	}
}

func (r *rateLimiter) resetCounters() {
	r.Lock()
	defer r.Unlock()
	r.requests = map[string]int{}
}
