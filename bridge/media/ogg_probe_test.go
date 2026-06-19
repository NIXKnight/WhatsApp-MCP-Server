package media

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

// genOgg uses ffmpeg to synthesize an OGG Opus clip of the given whole-second
// duration. The test is skipped when ffmpeg is unavailable.
func genOgg(t *testing.T, seconds int) []byte {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available; skipping OGG Opus duration test")
	}
	out := filepath.Join(t.TempDir(), "clip.ogg")
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration="+strconv.Itoa(seconds),
		"-c:a", "libopus", "-b:a", "24k", out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg failed: %v\n%s", err, b)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read generated clip: %v", err)
	}
	return data
}

// TestAnalyzeOggOpusDuration is a regression guard for the outbound voice-note
// duration bug: AnalyzeOggOpus must parse a minimal OpusHead (channel mapping
// family 0, 19-byte packet) and return the real duration, rather than erroring
// out and forcing the caller's hardcoded fallback.
func TestAnalyzeOggOpusDuration(t *testing.T) {
	for _, want := range []uint32{3, 7, 12} {
		data := genOgg(t, int(want))
		secs, wf, err := AnalyzeOggOpus(data)
		if err != nil {
			t.Fatalf("%ds clip: unexpected error: %v", want, err)
		}
		if secs != want {
			t.Errorf("%ds clip: got duration %d, want %d", want, secs, want)
		}
		if len(wf) != 64 {
			t.Errorf("%ds clip: waveform length = %d, want 64", want, len(wf))
		}
	}
}
