package media

import (
	"context"
	"errors"
	"strings"

	"go.mau.fi/whatsmeow"
)

// ClassifyDownloadError reports whether a download error is structurally
// hopeless — i.e. retrying it is futile because the media descriptor itself is
// missing (no URL / media key / direct path, or nothing downloadable in the
// message). Such errors should immediately retire the row.
//
// Crucially, transient HTTP failures (403/404/410) are NOT structural. They are
// almost always a stale signed URL / direct path that a later download call
// refreshes, so they must be retried across cycles and only retired once they
// exhaust the attempt budget. When an error cannot be positively classified as
// structural it is treated as retryable, and the caller's maxAttempts bounds it.
func ClassifyDownloadError(err error) (structural bool) {
	if err == nil {
		return false
	}
	// whatsmeow sentinels that mean "there is nothing to download".
	if errors.Is(err, whatsmeow.ErrNoURLPresent) ||
		errors.Is(err, whatsmeow.ErrNothingDownloadableFound) {
		return true
	}
	// Bridge-side guard in Download: the stored descriptor is incomplete (a
	// common history-sync case where crypto fields never arrived). Matched by
	// substring because Download returns a formatted, non-sentinel error.
	msg := err.Error()
	if strings.Contains(msg, "incomplete media metadata") ||
		strings.Contains(msg, "missing media info") {
		return true
	}
	return false
}

// DownloadErrorClass returns a short, log-safe classification of a download
// error. It never contains a signed URL, direct path, or media key — only a
// coarse class suitable for structured logs and operator triage.
func DownloadErrorClass(err error) string {
	if err == nil {
		return "ok"
	}
	switch {
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403):
		return "http_403"
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404):
		return "http_404"
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410):
		return "http_410"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case ClassifyDownloadError(err):
		return "structural"
	default:
		return "transient"
	}
}
