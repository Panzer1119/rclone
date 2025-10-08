package dedupe

import (
	"bytes"
	"io"
	"testing"

	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
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

func TestRabinChunkerDeterministic(t *testing.T) {
	// Verify that the same data produces the same chunks
	data := bytes.Repeat([]byte("deterministic test "), 5000)

	var chunks1, chunks2 [][]byte

	// First pass
	reader1 := bytes.NewReader(data)
	chunker1 := newRabinChunker(reader1, 4*1024*1024)
	for {
		chunk, err := chunker1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading chunk: %v", err)
		}
		chunks1 = append(chunks1, chunk)
	}

	// Second pass
	reader2 := bytes.NewReader(data)
	chunker2 := newRabinChunker(reader2, 4*1024*1024)
	for {
		chunk, err := chunker2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading chunk: %v", err)
		}
		chunks2 = append(chunks2, chunk)
	}

	// Verify same number of chunks
	if len(chunks1) != len(chunks2) {
		t.Errorf("Different number of chunks: %d vs %d", len(chunks1), len(chunks2))
	}

	// Verify chunks are identical
	for i := range chunks1 {
		if !bytes.Equal(chunks1[i], chunks2[i]) {
			t.Errorf("Chunk %d differs", i)
		}
	}

	t.Logf("Deterministic chunking verified with %d chunks", len(chunks1))
}

// TestIntegration runs integration tests for the backend
func TestIntegration(t *testing.T) {
	if *fstest.RemoteName == "" {
		t.Skip("Skipping integration test without remote")
	}
	fstests.Run(t, &fstests.Opt{
		RemoteName: *fstest.RemoteName,
		NilObject:  (*Object)(nil),
	})
}

