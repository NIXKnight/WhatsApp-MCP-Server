package api

import (
	"regexp"
	"strings"
)

// jidRe validates WhatsApp JIDs accepted by the API.
var jidRe = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|g\.us|lid)$`)

// normaliseJID converts a bare phone number to a WhatsApp individual JID.
// If the input already contains @, it is returned unchanged.
func normaliseJID(s string) string {
	if strings.Contains(s, "@") {
		return s
	}
	// Strip any non-digit characters (spaces, dashes, plus signs).
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	if digits == "" {
		return s
	}
	return digits + "@s.whatsapp.net"
}
