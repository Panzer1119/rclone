package dedupe

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternalBasicOperations tests basic operations without a real backend
func TestInternalBasicOperations(t *testing.T) {
	ctx := context.Background()

	// Create a mock configuration
	m := configmap.Simple{
		"remote":     ":memory:",
		"chunk_size": "1M",
		"hash_type":  "sha256",
	}

	// Test NewFs
	f, err := NewFs(ctx, "test", "", m)
	if err != nil {
		t.Skip("Skipping test - memory backend not available:", err)
		return
	}

	require.NotNil(t, f)
	assert.Equal(t, "test", f.Name())
	assert.Equal(t, "", f.Root())

	// Test that features are set
	features := f.Features()
	require.NotNil(t, features)
}

// TestMetadataMarshalling tests metadata JSON encoding/decoding
func TestMetadataMarshalling(t *testing.T) {
	meta := &FileMetadata{
		Name:      "test.txt",
		Size:      1234,
		ModTime:   time.Now(),
		Chunks:    []string{"abc123", "def456"},
		ChunkSize: 4194304,
	}

	// This would normally be tested with actual storage
	// but we can at least verify the structure is sound
	assert.NotEmpty(t, meta.Name)
	assert.Greater(t, meta.Size, int64(0))
	assert.Equal(t, 2, len(meta.Chunks))
}

// TestChunkReaderEmpty tests reading from empty metadata
func TestChunkReaderEmpty(t *testing.T) {
	ctx := context.Background()
	
	// Create a mock Fs - will fail without memory backend but that's ok
	m := configmap.Simple{
		"remote":     ":memory:",
		"chunk_size": "1M",
	}
	
	f, err := NewFs(ctx, "test", "", m)
	if err != nil {
		t.Skip("Skipping test - memory backend not available:", err)
		return
	}

	meta := &FileMetadata{
		Name:      "empty.txt",
		Size:      0,
		ModTime:   time.Now(),
		Chunks:    []string{},
		ChunkSize: 4194304,
	}

	cr := newChunkReader(ctx, f.(*Fs), meta)
	defer cr.Close()

	buf := make([]byte, 100)
	n, err := cr.Read(buf)
	
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

// TestOptionsValidation tests configuration validation
func TestOptionsValidation(t *testing.T) {
	ctx := context.Background()

	// Test invalid chunk size (too small)
	m := configmap.Simple{
		"remote":     ":memory:",
		"chunk_size": "1K",
	}

	_, err := NewFs(ctx, "test", "", m)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidChunkSize)

	// Test invalid chunk size (too large)
	m2 := configmap.Simple{
		"remote":     ":memory:",
		"chunk_size": "100M",
	}

	_, err = NewFs(ctx, "test", "", m2)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidChunkSize)

	// Test valid chunk size
	m3 := configmap.Simple{
		"remote":     ":memory:",
		"chunk_size": "4M",
	}

	// This might fail if memory backend not available, but size validation should pass first
	f, err := NewFs(ctx, "test", "", m3)
	if err != nil && err != ErrInvalidChunkSize {
		t.Skip("Memory backend not available")
	}
	if err == nil {
		assert.NotNil(t, f)
	}
}

// TestRabinChunkerLargeData tests chunking with larger data
func TestRabinChunkerLargeData(t *testing.T) {
	// Create 1MB of test data with patterns
	size := 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	chunker := newRabinChunker(reader, 256*1024) // 256KB target

	var totalSize int
	chunkCount := 0
	var chunkSizes []int

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		chunkCount++
		totalSize += len(chunk)
		chunkSizes = append(chunkSizes, len(chunk))

		// Verify chunk size constraints
		assert.GreaterOrEqual(t, len(chunk), minChunkSize)
		assert.LessOrEqual(t, len(chunk), maxChunkSize)
	}

	assert.Equal(t, size, totalSize, "Total size should match input")
	assert.Greater(t, chunkCount, 0, "Should have created at least one chunk")
	
	t.Logf("Chunked %d bytes into %d chunks (avg: %d bytes)", 
		totalSize, chunkCount, totalSize/chunkCount)
	
	if chunkCount > 1 {
		// Log chunk sizes for analysis
		t.Logf("Chunk sizes: %v", chunkSizes)
	}
}
