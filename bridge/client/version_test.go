package client

import (
	"testing"

	"go.mau.fi/whatsmeow/store"
)

func TestEmbeddedWAVersion(t *testing.T) {
	const want = "2.3000.1049558099"

	if got := store.GetWAVersion().String(); got != want {
		t.Fatalf("embedded WhatsApp version = %q, want %q", got, want)
	}
}
