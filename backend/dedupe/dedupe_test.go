package dedupe

import (
	"bytes"
	"io"
	"testing"
)

func TestRabinChunker(t *testing.T) {
	// Create test data
	data := bytes.Repeat([]byte("test data "), 10000) // ~90KB
	reader := bytes.NewReader(data)

	chunker := newRabinChunker(reader, 4*1024*1024)

	var totalSize int
	chunkCount := 0

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading chunk: %v", err)
		}

		chunkCount++
		totalSize += len(chunk)

		// Verify chunk size constraints
		if len(chunk) < minChunkSize && chunkCount > 1 {
			t.Errorf("Chunk %d is too small: %d bytes", chunkCount, len(chunk))
		}
		if len(chunk) > maxChunkSize {
			t.Errorf("Chunk %d is too large: %d bytes", chunkCount, len(chunk))
		}
	}

	if totalSize != len(data) {
		t.Errorf("Total size mismatch: got %d, want %d", totalSize, len(data))
	}

	t.Logf("Created %d chunks from %d bytes", chunkCount, len(data))
}

func TestRabinChunkerEmpty(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	chunker := newRabinChunker(reader, 4*1024*1024)

	chunk, err := chunker.Next()
	if err != io.EOF {
		t.Errorf("Expected EOF, got: %v", err)
	}
	if chunk != nil {
		t.Errorf("Expected nil chunk, got: %v", chunk)
	}
}

func TestRabinChunkerSmallData(t *testing.T) {
	data := []byte("small data")
	reader := bytes.NewReader(data)
	chunker := newRabinChunker(reader, 4*1024*1024)

	chunk, err := chunker.Next()
	if err != nil {
		t.Fatalf("Error reading chunk: %v", err)
	}

	if !bytes.Equal(chunk, data) {
		t.Errorf("Chunk data mismatch")
	}

	// Should get EOF on next read
	_, err = chunker.Next()
	if err != io.EOF {
		t.Errorf("Expected EOF, got: %v", err)
	}
}
