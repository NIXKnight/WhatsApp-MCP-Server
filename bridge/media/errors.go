package media

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// LocalDestError marks a failure to create or write the *local destination*
// (the output directory or the target file) as distinct from a failure to
// fetch/decrypt the media from WhatsApp.
//
// This distinction matters because the two failures have opposite remedies and
// opposite effects on media-availability state:
//
//   - A WhatsApp media error (a stale signed URL, a 403/404/410, a missing
//     descriptor) is a property of the media and feeds the retry/permanent-fail
//     bookkeeping (download_attempts, download_permanently_failed).
//   - A LocalDestError is a property of the *caller-supplied destination* — an
//     unwritable output_dir, a read-only mount, a full disk. The media may be
//     perfectly available; only the place we were asked to put it is the
//     problem. Such a failure MUST NOT bump download_attempts or set
//     download_permanently_failed, and MUST NOT surface as 410/503. It is the
//     caller's output_dir that is at fault, and the response says so.
//
// Dir is the directory we failed to create or write into; Op names the failing
// operation ("create media dir", "write file", "copy cached media"). Err is the
// underlying *fs.PathError (or other os error).
type LocalDestError struct {
	Op  string
	Dir string
	Err error
}

func (e *LocalDestError) Error() string {
	return fmt.Sprintf("%s %s: %v", e.Op, e.Dir, e.Err)
}

func (e *LocalDestError) Unwrap() error { return e.Err }

// newLocalDestError wraps a local filesystem failure, recording the directory
// at fault. For an *fs.PathError originating from os.MkdirAll/os.Create/
// os.WriteFile the directory is derived from the offending path; otherwise dir
// is used verbatim.
func newLocalDestError(op, dir string, err error) *LocalDestError {
	return &LocalDestError{Op: op, Dir: dir, Err: err}
}

// AsLocalDestError reports whether err represents a failure to write the local
// destination rather than a WhatsApp media failure, returning the typed error
// when so.
//
// Primary detection is the typed LocalDestError that Download and CopyFile emit
// at every local-write site. As a defensive fallback it also recognises a bare
// *fs.PathError from a filesystem mutation (mkdir/open/create/write) or any
// os.IsPermission error, so a future local-write site that forgets to wrap is
// still classified correctly and never mislabeled as a media failure.
func AsLocalDestError(err error) (*LocalDestError, bool) {
	if err == nil {
		return nil, false
	}

	var lde *LocalDestError
	if errors.As(err, &lde) {
		return lde, true
	}

	// Defensive fallback: a raw path error from a write-side syscall, or a
	// permission error, is a local destination problem even unwrapped.
	var pe *fs.PathError
	if errors.As(err, &pe) {
		switch pe.Op {
		case "mkdir", "open", "openat", "create", "write", "stat":
			// "open"/"stat" only count as a *destination* problem when the
			// access was denied (a read-only/forbidden mount); a plain
			// not-exist is a cache miss, handled by the caller, not here.
			if pe.Op == "mkdir" || pe.Op == "create" || pe.Op == "write" || os.IsPermission(pe) {
				return newLocalDestError(pe.Op, filepath.Dir(pe.Path), pe), true
			}
		}
	}
	if os.IsPermission(err) {
		return newLocalDestError("write", "", err), true
	}
	return nil, false
}
