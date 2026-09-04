// Package indexer is part of L2 capture: it extracts and classifies structured
// signals (URLs, and in later phases other entities) from raw message text at
// capture time.
package indexer

import (
	"net/url"
	"regexp"
	"strings"
)

var urlRegex = regexp.MustCompile(`https?://[^\s<>"{}|\\^` + "`" + `\[\]]+`)

var platformMap = map[string]string{
	"youtube.com":       "youtube",
	"www.youtube.com":   "youtube",
	"youtu.be":          "youtube",
	"m.youtube.com":     "youtube",
	"facebook.com":      "facebook",
	"www.facebook.com":  "facebook",
	"fb.com":            "facebook",
	"m.facebook.com":    "facebook",
	"instagram.com":     "instagram",
	"www.instagram.com": "instagram",
	"twitter.com":       "twitter",
	"www.twitter.com":   "twitter",
	"x.com":             "twitter",
	"www.x.com":         "twitter",
	"github.com":        "github",
	"www.github.com":    "github",
	"linkedin.com":      "linkedin",
	"www.linkedin.com":  "linkedin",
	"tiktok.com":        "tiktok",
	"www.tiktok.com":    "tiktok",
}

// ExtractedLink represents a URL found in text with its platform classification.
type ExtractedLink struct {
	URL      string
	Platform string
}

// ExtractLinks finds all URLs in text and classifies them by platform.
// Duplicate URLs (after trailing-punctuation cleanup) are returned once.
func ExtractLinks(text string) []ExtractedLink {
	matches := urlRegex.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}

	var links []ExtractedLink
	seen := make(map[string]bool)

	for _, raw := range matches {
		// Clean trailing punctuation.
		raw = strings.TrimRight(raw, ".,;:!?)")

		if seen[raw] {
			continue
		}
		seen[raw] = true

		platform := classifyURL(raw)
		links = append(links, ExtractedLink{URL: raw, Platform: platform})
	}

	return links
}

func classifyURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "other"
	}

	host := strings.ToLower(parsed.Hostname())
	if p, ok := platformMap[host]; ok {
		return p
	}

	return "other"
}
