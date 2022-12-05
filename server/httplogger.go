// This file is copied from Flashbots repo 'go-utils' version 0.4.5, path "github.com/flashbots/go-utils/httplogger"
// The LoggingMiddlewareLogrus() function has been changed to additionally log the 'remoteAddress' of HTTP requests
package server

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/sirupsen/logrus"
)

// responseWriter is a minimal wrapper for http.ResponseWriter that allows the
// written HTTP status code to be captured for logging.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
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

// LoggingMiddleware logs the incoming HTTP request & its duration.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)

					method := ""
					url := ""
					if r != nil {
						method = r.Method
						url = r.URL.EscapedPath()
					}

					log.Error(fmt.Sprintf("http request panic: %s %s", method, url),
						"err", err,
						"trace", string(debug.Stack()),
					)
				}
			}()
			start := time.Now()
			wrapped := wrapResponseWriter(w)
			next.ServeHTTP(wrapped, r)
			log.Info(fmt.Sprintf("http: %s %s %d", r.Method, r.URL.EscapedPath(), wrapped.status),
				"status", wrapped.status,
				"method", r.Method,
				"path", r.URL.EscapedPath(),
				"duration", time.Since(start).Seconds(),
			)
		},
	)
}

// LoggingMiddlewareLogrus logs the incoming HTTP request & its duration.
func LoggingMiddlewareLogrus(logger *logrus.Entry, next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)

					method := ""
					url := ""
					if r != nil {
						method = r.Method
						url = r.URL.EscapedPath()
					}

					logger.WithFields(logrus.Fields{
						"err":    err,
						"trace":  string(debug.Stack()),
						"method": r.Method,
					}).Error(fmt.Sprintf("http request panic: %s %s", method, url))
				}
			}()
			start := time.Now()
			wrapped := wrapResponseWriter(w)

			next.ServeHTTP(wrapped, r)

			statusCode := wrapped.Status()
			endpointLog := logger.WithFields(logrus.Fields{
				"status":        wrapped.Status(),
				"method":        r.Method,
				"path":          r.URL.EscapedPath(),
				"remoteAddress": r.RemoteAddr,
				"duration":      time.Since(start).Seconds(),
			})

			urlInfo := fmt.Sprintf("http: %s %s %d", r.Method, r.URL.EscapedPath(), statusCode)
			if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
				endpointLog.Trace(urlInfo)
			} else {
				endpointLog.Warn(urlInfo)
			}
		},
	)
}
