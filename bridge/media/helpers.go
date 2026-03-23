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
