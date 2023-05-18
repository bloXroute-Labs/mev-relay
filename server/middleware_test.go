package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/google/uuid"
	"github.com/justinas/alice"
	"github.com/stretchr/testify/require"
)

func Test_rateLimitMiddleware(t *testing.T) {
	allowedRequests := 4
	cleanupInterval := time.Second * 2
	neverCheckHeader := "never-check-header"

	// create server with handler wrapper in rate-limiting middleware
	svr := httptest.NewServer(alice.New(rateLimitMiddleware(testLog, newRateLimiter(cleanupInterval, allowedRequests),
		mockLimiterCaller(neverCheckHeader))).
		ThenFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte("{}"))
			require.NoError(t, err)
		}))
	defer svr.Close()

	client := http.DefaultClient

	for _, shouldRateLimit := range []bool{false, true} {
		for i := 0; i <= allowedRequests+2; i++ {
			req, err := http.NewRequest(http.MethodGet, svr.URL, nil)
			require.NoError(t, err)
			if !shouldRateLimit {
				req.Header.Set(neverCheckHeader, "never!")
			}

			resp, err := client.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			if shouldRateLimit && i == allowedRequests {
				require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
				time.Sleep(cleanupInterval)
				continue
			}

			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
	}
}

func Test_tierRateLimitMiddleware(t *testing.T) {
	t.Run("enterprise account", func(t *testing.T) {
		enterpriseAccID := uuid.NewString()
		ctx := context.WithValue(context.TODO(), authInfoKey,
			newAuthInfo(headerAuth).addAccountID(enterpriseAccID).addAccountTier(sdnmessage.ATierEnterprise))

		backend := newTestBackend(t, 1, 1, &database.MockDB{})
		testTierRateLimitHelper(t, ctx, enterpriseExternalBuilderAllowedSubmitBlockReqPerSec, time.Second, backend)
	})
	t.Run("elite account", func(t *testing.T) {
		eliteAccID := uuid.NewString()
		ctx := context.WithValue(context.TODO(), authInfoKey,
			newAuthInfo(headerAuth).addAccountID(eliteAccID).addAccountTier(sdnmessage.ATierElite))

		backend := newTestBackend(t, 1, 1, &database.MockDB{})
		testTierRateLimitHelper(t, ctx, eliteExternalBuilderAllowedSubmitBlockReqPerSec, time.Second, backend)

		ctx = context.WithValue(context.TODO(), authInfoKey,
			newAuthInfo(headerAuth).addAccountID(eliteAccID).addAccountTier(sdnmessage.ATierUltra))

		backend = newTestBackend(t, 1, 1, &database.MockDB{})
		testTierRateLimitHelper(t, ctx, eliteExternalBuilderAllowedSubmitBlockReqPerSec, time.Second, backend)
	})
	t.Run("ultra account", func(t *testing.T) {
		ultraAccID := uuid.NewString()
		ultraAccIDs := syncmap.NewStringMapOf[bool]()
		ultraAccIDs.Store(ultraAccID, true)
		ctx := context.WithValue(context.TODO(), authInfoKey,
			newAuthInfo(headerAuth).addAccountID(ultraAccID).addAccountTier(sdnmessage.ATierElite))

		backend := newTestBackend(t, 1, 1, &database.MockDB{})
		backend.boost.ultraBuilderAccountIDs = ultraAccIDs
		testTierRateLimitHelper(t, ctx, ultraExternalBuilderAllowedSubmitBlockReqPerSec, time.Second, backend)

		ctx = context.WithValue(context.TODO(), authInfoKey,
			newAuthInfo(headerAuth).addAccountID(ultraAccID).addAccountTier(sdnmessage.ATierUltra))

		backend = newTestBackend(t, 1, 1, &database.MockDB{})
		backend.boost.ultraBuilderAccountIDs = ultraAccIDs
		testTierRateLimitHelper(t, ctx, ultraExternalBuilderAllowedSubmitBlockReqPerSec, time.Second, backend)
	})
	t.Run("no rate limit for specific builder", func(t *testing.T) {
		ultraAccID := uuid.NewString()
		ultraAccIDs := syncmap.NewStringMapOf[bool]()
		ultraAccIDs.Store(ultraAccID, true)
		ctx := context.WithValue(context.TODO(), authInfoKey,
			newAuthInfo(headerAuth).addAccountID(ultraAccID).addAccountTier(sdnmessage.ATierElite))

		backend := newTestBackend(t, 1, 1, &database.MockDB{})
		backend.boost.ultraBuilderAccountIDs = ultraAccIDs
		backend.boost.noRateLimitUltraBuilderAccIDs = ultraAccIDs

		testTierRateLimitHelper(t, ctx, -1, 0, backend)
	})
}

func testTierRateLimitHelper(t *testing.T, ctx context.Context, limit int, cleanupInterval time.Duration, backend *testBackend) {
	t.Helper()

	svr := httptest.NewServer(alice.New(mockInjectCtxMiddleware(ctx), backend.boost.tierRateLimitMiddleware(newTierRateLimiters())).
		ThenFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte("{}"))
			require.NoError(t, err)
		}))
	defer svr.Close()

	// should not rate limit
	if limit <= 0 {
		// use the bigger number to make sure it does not rate limit
		limit = ultraExternalBuilderAllowedSubmitBlockReqPerSec * 2
		for i := 0; i <= limit+1; i++ {
			resp, err := svr.Client().Get(svr.URL)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}

		return
	}

	for i := 0; i <= limit+1; i++ {
		resp, err := svr.Client().Get(svr.URL)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		if i == limit {
			require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
			time.Sleep(cleanupInterval)
			continue
		}
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
}

func mockInjectCtxMiddleware(ctx context.Context) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r.WithContext(ctx))
			},
		)
	}
}

func mockLimiterCaller(neverCheckHeader string) func(r *http.Request) (string, bool) {
	return func(r *http.Request) (string, bool) {
		h := r.Header.Get(neverCheckHeader)
		if h != "" {
			return "", false
		}
		return "caller", true
	}
}
