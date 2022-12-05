package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewRateLimiter(t *testing.T) {
	rl := newRateLimiter(time.Second, 4)

	assert.Equal(t, time.Second, rl.cleanupInterval)
	assert.Equal(t, 4, rl.requestsLimit)
}

func TestRateLimiterCanProcess(t *testing.T) {
	rl := rateLimiter{requestsLimit: 4, requests: map[string]int{"account1": 1, "account2": 4}}
	assert.True(t, rl.canProcess("account1"))
	assert.Equal(t, 2, rl.requests["account1"])
	assert.False(t, rl.canProcess("account2"))
	assert.True(t, rl.canProcess("account3"))
	assert.Equal(t, 1, rl.requests["account3"])
}

func TestRateLimiterResetCounters(t *testing.T) {
	rl := rateLimiter{requests: map[string]int{"account1": 1}}
	rl.resetCounters()
	assert.Equal(t, 0, len(rl.requests))
}

func TestThatRequestCounterCleanedAfterCleanup(t *testing.T) {
	rl := newRateLimiter(1, 1)
	rl.canProcess("account1")
	assert.Equal(t, 1, rl.requests["account1"])
	time.Sleep(2 * time.Second)
	assert.Equal(t, 0, len(rl.requests))
}
