package connection

import "sync/atomic"

// KeepaliveTracker counts consecutive keepalive failures. After MaxFailures
// consecutive timeouts, the caller should force a reconnect. The counter is
// safe for concurrent use.
type KeepaliveTracker struct {
	failures    atomic.Int32
	MaxFailures int32
}

// NewKeepaliveTracker returns a tracker that signals a reconnect once
// maxFailures consecutive keepalive failures have been recorded.
func NewKeepaliveTracker(maxFailures int32) *KeepaliveTracker {
	return &KeepaliveTracker{MaxFailures: maxFailures}
}

// RecordFailure increments the consecutive-failure counter and returns the new
// count.
func (kt *KeepaliveTracker) RecordFailure() int32 {
	return kt.failures.Add(1)
}

// Reset clears the consecutive-failure counter.
func (kt *KeepaliveTracker) Reset() {
	kt.failures.Store(0)
}

// ShouldReconnect reports whether the failure count has reached MaxFailures.
func (kt *KeepaliveTracker) ShouldReconnect() bool {
	return kt.failures.Load() >= kt.MaxFailures
}
