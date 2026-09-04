package media

import (
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
)

// SaveUploadedFile writes the multipart file upload to a temporary file inside
// dataDir and returns the absolute path plus a cleanup function that removes
// the file. The caller must call cleanup() when the file is no longer needed.
func SaveUploadedFile(fh *multipart.FileHeader, dataDir string) (path string, cleanup func(), err error) {
	src, err := fh.Open()
	if err != nil {
		return "", nil, fmt.Errorf("open upload: %w", err)
	}
	defer src.Close()

	// Create a temp directory inside dataDir to keep uploaded files together.
	tmpDir := filepath.Join(dataDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return "", nil, fmt.Errorf("create tmp dir: %w", err)
	}

	// Use the original filename suffix so media type detection by extension works.
	ext := filepath.Ext(fh.Filename)
	if ext == "" {
		ext = ".bin"
	}

	f, err := os.CreateTemp(tmpDir, "upload_*"+ext)
	if err != nil {
		return "", nil, fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(f, src); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", nil, fmt.Errorf("write temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", nil, fmt.Errorf("close temp file: %w", err)
	}

	tmpPath := f.Name()
	cleanupFn := func() { os.Remove(tmpPath) }
	return tmpPath, cleanupFn, nil
}

// DetectTypeFromFilename returns a media type string ("image", "video",
// "audio", "document") derived from the file extension of name.
func DetectTypeFromFilename(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	if ext != "" {
		ext = ext[1:] // strip leading dot
	}
	return extToMediaType(ext)
}

// CopyFile copies the file at src to dst, creating dst's parent directory if
// needed. It returns the number of bytes written. The destination is created
// with 0600 permissions to match the perms Download writes media with. Used to
// serve a cached media copy into a caller-requested output directory without
// re-fetching it from the network.
//
// Failures are split by side: anything wrong with the *destination* (creating
// its directory, creating/writing/closing the target file) is returned as a
// *LocalDestError so a caller serving an already-cached file can tell "the
// bytes exist, but the requested output_dir is unwritable" apart from "the
// cached source is gone". A failure to open src is returned as a plain error,
// since that means the cache copy itself vanished (a genuine cache miss).
func CopyFile(src, dst string) (int64, error) {
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0700); err != nil {
		return 0, newLocalDestError("create dest dir", dstDir, err)
	}

	in, err := os.Open(src)
	if err != nil {
		// Source is the cached copy; if it cannot be opened the cache entry is
		// effectively gone. Not a destination problem.
		return 0, fmt.Errorf("open source %q: %w", src, err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return 0, newLocalDestError("create dest file", dstDir, err)
	}

	n, err := io.Copy(out, in)
	if err != nil {
		out.Close()
		os.Remove(dst)
		return 0, newLocalDestError("write dest file", dstDir, err)
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return 0, newLocalDestError("close dest file", dstDir, err)
	}
	return n, nil
}

// ExpandTilde replaces a leading "~/" with the user's home directory path.
// If the path does not start with "~/" it is returned unchanged.
func ExpandTilde(path string) string {
	if !strings.HasPrefix(path, "~/") {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return path
	}
	return filepath.Join(home, path[2:])
}
