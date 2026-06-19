// Package analyzer implements the L2 outbound client for the L3 transcriber's
// media-analysis endpoint. It forwards a single message reference over HTTP and
// returns the analysis result (extracted frame paths plus any transcription) so
// that POST /api/media/analyze can act as a thin proxy without doing any media
// (ffmpeg) work itself.
//
// The HTTP contract is fixed:
//
//	POST {baseURL}/analyze  body {"chat_jid":"...","message_id":"..."}
//	  -> 200 {"frame_paths":["..."],"transcription":"...","duration":<number>,"frame_count":<int>}
//	GET  {baseURL}/health
//
// Every request is bounded by the client Timeout and the caller's context. The
// analysis is heavy (video frame extraction, audio transcription) so the timeout
// is generous; failures surface as errors rather than being retried.
package analyzer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client forwards media-analysis requests to the L3 transcriber service over
// HTTP. It holds no store reference: the proxy is intentionally free of schema
// coupling.
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

type analyzeRequest struct {
	ChatJID   string `json:"chat_jid"`
	MessageID string `json:"message_id"`
}

// AnalyzeResult is the JSON response returned by the transcriber's /analyze
// endpoint: the paths of any extracted video frames, the audio transcription,
// the media duration in seconds, and the number of frames extracted.
type AnalyzeResult struct {
	FramePaths    []string `json:"frame_paths"`
	Transcription string   `json:"transcription"`
	Duration      float64  `json:"duration"`
	FrameCount    int      `json:"frame_count"`
}

// Analyze forwards a single (chatJID, messageID) reference to the transcriber's
// /analyze endpoint and returns the decoded result.
//
// It returns an error when the request cannot be built or sent, or when the
// service responds with a non-200 status. This is a single attempt: the request
// is bounded by the client Timeout and the supplied context, and is never
// retried.
func (c *Client) Analyze(ctx context.Context, chatJID, messageID string) (AnalyzeResult, error) {
	var out AnalyzeResult

	body, err := json.Marshal(analyzeRequest{ChatJID: chatJID, MessageID: messageID})
	if err != nil {
		return out, fmt.Errorf("encode analyze request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/analyze", bytes.NewReader(body))
	if err != nil {
		return out, fmt.Errorf("build analyze request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return out, fmt.Errorf("analyze request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("analyzer returned status %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("decode analyze response: %w", err)
	}

	return out, nil
}
