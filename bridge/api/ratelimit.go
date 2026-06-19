package api

import (
	"math/rand/v2"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter is a token-bucket limiter with optional human-timing jitter,
// applied as chi middleware to the outbound send routes. It is anti-ban
// protection: WhatsApp penalises bursts of automated sends, so the bridge
// throttles its own send rate and adds small random delays to avoid a robotic,
// perfectly-periodic signature.
type RateLimiter struct {
	limiter  *rate.Limiter
	jitterMs int
}

// NewRateLimiter builds a limiter permitting messagesPerSecond sustained sends
// with a burst allowance, adding up to jitterMs milliseconds of random delay
// per request. A non-positive jitterMs disables jitter.
func NewRateLimiter(messagesPerSecond float64, burst, jitterMs int) *RateLimiter {
	if burst < 1 {
		burst = 1
	}
	return &RateLimiter{
		limiter:  rate.NewLimiter(rate.Limit(messagesPerSecond), burst),
		jitterMs: jitterMs,
	}
}

// Middleware applies rate limiting to the wrapped handler. Requests that exceed
// the bucket are rejected with 429; permitted requests are delayed by a random
// jitter before proceeding.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.limiter.Allow() {
			writeError(w, http.StatusTooManyRequests, "too many messages, please slow down", "RATE_LIMITED")
			return
		}

		if rl.jitterMs > 0 {
			time.Sleep(time.Duration(rand.IntN(rl.jitterMs)) * time.Millisecond)
		}

		next.ServeHTTP(w, r)
	})
}
