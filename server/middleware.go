package server

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/sirupsen/logrus"
)

// responseWriter is a minimal wrapper for http.ResponseWriter that allows the
// written HTTP status code to be captured for logging.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijack not supported")
	}
	return h.Hijack()
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w}
}

func (rw *responseWriter) Status() int {
	return rw.status
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}

	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
	rw.wroteHeader = true
}

// recoveryMiddlewareWithLogs is an HTTP middleware that recovers from a panic,
// logs the panic, writes http.StatusInternalServerError, and
// continues to the next handler.
func recoveryMiddlewareWithLogs(logger *logrus.Entry, next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("Ooops! Something went terribly wrong!"))
					logger.WithFields(logrus.Fields{
						"trace":  string(debug.Stack()),
						"method": r.Method,
						"url":    r.URL.EscapedPath(),
					}).Error(err)
				}
			}()
			start := time.Now()
			wrapped := wrapResponseWriter(w)

			next.ServeHTTP(wrapped, r)

			logger.WithFields(logrus.Fields{
				"status":   wrapped.Status(),
				"method":   r.Method,
				"path":     r.URL.EscapedPath(),
				"duration": time.Since(start).Seconds(),
			}).Trace("finished handling the request")
		},
	)
}

const (
	enterpriseExternalBuilderAllowedSubmitBlockReqPerSec = 1
	eliteExternalBuilderAllowedSubmitBlockReqPerSec      = 2
	ultraExternalBuilderAllowedSubmitBlockReqPerSec      = 4
)

// tierRateLimiters holds set of rate limiters for different acc tiers
type tierRateLimiters struct {
	enterpriseRL, eliteRL, ultraRL *rateLimiter
}

func newTierRateLimiters() *tierRateLimiters {
	return &tierRateLimiters{
		enterpriseRL: newRateLimiter(time.Second, enterpriseExternalBuilderAllowedSubmitBlockReqPerSec),
		eliteRL:      newRateLimiter(time.Second, eliteExternalBuilderAllowedSubmitBlockReqPerSec),
		ultraRL:      newRateLimiter(time.Second, ultraExternalBuilderAllowedSubmitBlockReqPerSec),
	}
}

// tierRateLimitMiddleware is same as the rateLimitMiddleware,
// but applies different rate limits to different account tiers.
func (m *BoostService) tierRateLimitMiddleware(trl *tierRateLimiters) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				// no acc info, thus nothing to check
				authInfo, ok := r.Context().Value(authInfoKey).(authInfo)
				if !ok {
					next.ServeHTTP(w, r)
					return
				}
				// no info about account's tier
				tier, ok := authInfo[accountTier]
				if !ok {
					next.ServeHTTP(w, r)
					return
				}

				var canProcess bool
				if tier.(sdnmessage.AccountTier).IsElite() {
					ok, _ := m.ultraBuilderAccountIDs.Load(authInfo[accountIDKey].(string))
					skipRateLimitCheck, _ := m.noRateLimitUltraBuilderAccIDs.Load(authInfo[accountIDKey].(string))
					if skipRateLimitCheck {
						canProcess = true
					} else if ok {
						canProcess = trl.ultraRL.canProcess(authInfo[accountIDKey].(string))
					} else {
						canProcess = trl.eliteRL.canProcess(authInfo[accountIDKey].(string))
					}
				} else {
					canProcess = trl.enterpriseRL.canProcess(authInfo[accountIDKey].(string))
				}

				if !canProcess {
					w.WriteHeader(http.StatusTooManyRequests)
					w.Write([]byte("rate limit exceeded"))
					m.log.WithFields(logrus.Fields{
						"caller": authInfo[accountIDKey],
						"url":    r.URL.EscapedPath(),
					}).Warn("reached rate limit")
					return
				}

				next.ServeHTTP(w, r)
			},
		)
	}
}

// rateLimitMiddleware is an HTTP middleware that returns an error
// in case the caller exceeds allowed limits.
func rateLimitMiddleware(logger *logrus.Entry, rl *rateLimiter, caller func(r *http.Request) (string, bool)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				id, shouldCheck := caller(r)
				if shouldCheck && !rl.canProcess(id) {
					w.WriteHeader(http.StatusTooManyRequests)
					w.Write([]byte("rate limit exceeded"))
					logger.WithFields(logrus.Fields{
						"caller": id,
						"url":    r.URL.EscapedPath(),
					}).Warn("reached rate limit")
					return
				}
				next.ServeHTTP(w, r)
			},
		)
	}
}

func getHeaderCaller(r *http.Request) (string, bool) {
	return common.GetIPXForwardedFor(r), true
}
