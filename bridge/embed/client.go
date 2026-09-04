// Package embed implements the L2 outbound client for the L3 embedder service.
// It fetches query-time vector embeddings over HTTP so that POST /api/search can
// perform true hybrid (FTS + pgvector) search when the caller supplies only text.
//
// The HTTP contract is fixed:
//
//	POST {baseURL}/embed  body {"text":"..."}  -> 200 {"embedding":[<384 floats>]}
//	GET  {baseURL}/health
//
// Every request is bounded by the client Timeout and the caller's context.
// Embedding failures are non-fatal at the call site: search falls back to
// FTS-only, so this client surfaces errors rather than retrying.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// Client fetches embeddings from the L3 embedder service over HTTP.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a Client targeting baseURL with the given per-request timeout.
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

type embedRequest struct {
	Text string `json:"text"`
}

type embedResponse struct {
	Embedding []float32 `json:"embedding"`
}

// Embed requests a single embedding for text from the embedder service.
//
// It returns an error when the request cannot be built or sent, when the
// service responds with a non-200 status, or when the returned vector does not
// have exactly store.EmbeddingDim dimensions. The dimension check ties the
// embedder output to the vector(384) schema constant, so a returned vector is
// always safe to hand to SearchHybrid without re-validation.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(embedRequest{Text: text})
	if err != nil {
		return nil, fmt.Errorf("encode embed request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedder returned status %d", resp.StatusCode)
	}

	var out embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode embed response: %w", err)
	}

	if len(out.Embedding) != store.EmbeddingDim {
		return nil, fmt.Errorf("embedder returned %d dimensions, want %d", len(out.Embedding), store.EmbeddingDim)
	}

	return out.Embedding, nil
}
