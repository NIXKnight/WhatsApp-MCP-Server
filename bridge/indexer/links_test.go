package indexer

import "testing"

func TestExtractLinks(t *testing.T) {
	tests := []struct {
		text     string
		expected []ExtractedLink
	}{
		{
			text:     "Check this out https://www.youtube.com/watch?v=abc123",
			expected: []ExtractedLink{{URL: "https://www.youtube.com/watch?v=abc123", Platform: "youtube"}},
		},
		{
			text:     "Follow me on https://twitter.com/user and https://github.com/user",
			expected: []ExtractedLink{{URL: "https://twitter.com/user", Platform: "twitter"}, {URL: "https://github.com/user", Platform: "github"}},
		},
		{
			text:     "Random link https://example.com/page",
			expected: []ExtractedLink{{URL: "https://example.com/page", Platform: "other"}},
		},
		{
			text:     "No links here",
			expected: nil,
		},
		{
			text:     "X.com link https://x.com/user/status/123",
			expected: []ExtractedLink{{URL: "https://x.com/user/status/123", Platform: "twitter"}},
		},
		{
			text:     "Short YT https://youtu.be/abc123",
			expected: []ExtractedLink{{URL: "https://youtu.be/abc123", Platform: "youtube"}},
		},
	}

	for _, tt := range tests {
		got := ExtractLinks(tt.text)
		if len(got) != len(tt.expected) {
			t.Errorf("ExtractLinks(%q): got %d links, want %d", tt.text, len(got), len(tt.expected))
			continue
		}
		for i, link := range got {
			if link.URL != tt.expected[i].URL || link.Platform != tt.expected[i].Platform {
				t.Errorf("ExtractLinks(%q)[%d]: got {%s, %s}, want {%s, %s}",
					tt.text, i, link.URL, link.Platform, tt.expected[i].URL, tt.expected[i].Platform)
			}
		}
	}
}
