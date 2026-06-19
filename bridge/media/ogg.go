// Package media is part of L2: it handles WhatsApp media upload, on-demand
// download, and OGG Opus analysis for the bridge.
package media

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// AnalyzeOggOpus parses an OGG Opus file and returns the audio duration in
// seconds and a 64-sample waveform suitable for WhatsApp PTT messages.
// It operates entirely in-process with no external commands.
func AnalyzeOggOpus(data []byte) (durationSeconds uint32, waveform []byte, err error) {
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	var lastGranule uint64
	// Opus granule positions are always at 48 kHz regardless of the input
	// sample rate stored in OpusHead, so this must never be overridden.
	const sampleRate uint32 = 48000
	var preSkip uint16
	var foundOpusHead bool
	var pageSamples []float64 // accumulated per-page loudness samples

	for i := 0; i < len(data); {
		if i+27 > len(data) {
			break
		}
		if string(data[i:i+4]) != "OggS" {
			i++
			continue
		}

		// Ogg page header layout:
		//   0-3  : capture pattern "OggS"
		//   4    : stream structure version (always 0)
		//   5    : header type
		//   6-13 : granule position (int64 LE)
		//  14-17 : bitstream serial number
		//  18-21 : page sequence number
		//  22-25 : checksum
		//  26   : number of page segments
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		numSegments := int(data[i+26])

		if i+27+numSegments > len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Sum segment lengths to get total payload size.
		payloadSize := 0
		for _, seg := range segmentTable {
			payloadSize += int(seg)
		}

		headerSize := 27 + numSegments
		pageSize := headerSize + payloadSize

		if i+pageSize > len(data) {
			break
		}

		pagePayload := data[i+headerSize : i+pageSize]

		// Parse OpusHead from the first logical page.
		if !foundOpusHead {
			headPos := bytes.Index(pagePayload, []byte("OpusHead"))
			if headPos >= 0 {
				hp := headPos + 8 // skip "OpusHead"
				if hp+12 <= len(pagePayload) {
					// Version (1B), Channels (1B), PreSkip (2B LE), SampleRate (4B LE)
					preSkip = binary.LittleEndian.Uint16(pagePayload[hp+2 : hp+4])
					foundOpusHead = true
				}
			}
		}

		// Track the highest valid granule position for duration calculation.
		if granulePos != 0xFFFFFFFFFFFFFFFF && granulePos > lastGranule {
			lastGranule = granulePos
		}

		// Collect a loudness sample per page for the waveform.
		if len(pagePayload) > 0 {
			loudness := computePageLoudness(pagePayload)
			pageSamples = append(pageSamples, loudness)
		}

		i += pageSize
	}

	if !foundOpusHead {
		return 0, nil, fmt.Errorf("OpusHead not found, may not be an Opus file")
	}
	if lastGranule == 0 {
		return 0, nil, fmt.Errorf("no valid granule positions found")
	}

	// Duration = (lastGranule - preSkip) / sampleRate
	if lastGranule > uint64(preSkip) {
		samples := lastGranule - uint64(preSkip)
		durationSeconds = uint32(samples / uint64(sampleRate))
		if durationSeconds == 0 {
			durationSeconds = 1
		}
	} else {
		durationSeconds = 1
	}

	waveform = buildWaveform(pageSamples, 64)
	return durationSeconds, waveform, nil
}

// computePageLoudness returns a simple RMS proxy for the page payload bytes.
// We treat raw bytes as audio amplitude proxies, which is imprecise but
// avoids a full Opus decoder dependency and produces a plausible waveform.
func computePageLoudness(payload []byte) float64 {
	if len(payload) == 0 {
		return 0
	}
	var sum float64
	for _, b := range payload {
		v := float64(b) - 128 // centre around zero
		sum += v * v
	}
	return math.Sqrt(sum / float64(len(payload)))
}

// buildWaveform down- or up-samples pageSamples to exactly n points and
// normalises to the range [0, 100] as a []byte.
func buildWaveform(samples []float64, n int) []byte {
	out := make([]byte, n)
	if len(samples) == 0 {
		return out
	}

	// Find maximum for normalisation.
	maxVal := 0.0
	for _, v := range samples {
		if v > maxVal {
			maxVal = v
		}
	}
	if maxVal == 0 {
		return out
	}

	// Map n output bins onto the input samples.
	for i := 0; i < n; i++ {
		srcIdx := float64(i) * float64(len(samples)) / float64(n)
		lo := int(srcIdx)
		hi := lo + 1
		if hi >= len(samples) {
			hi = len(samples) - 1
		}
		frac := srcIdx - float64(lo)
		v := samples[lo]*(1-frac) + samples[hi]*frac
		normalized := v / maxVal * 100
		if normalized > 100 {
			normalized = 100
		}
		out[i] = byte(normalized)
	}

	return out
}
