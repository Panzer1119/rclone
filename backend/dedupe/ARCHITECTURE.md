# Dedupe Backend Architecture

## Overview

The dedupe backend implements content-defined chunking and deduplication for rclone. This document explains the technical architecture and design decisions.

## Core Components

### 1. Rolling Hash Chunker (Rabin Fingerprinting)

**Purpose**: Split files into variable-size chunks based on content, not fixed boundaries.

**Implementation** (`rabinChunker`):
- Uses a rolling hash window of 48 bytes
- Computes hash: `hash = (hash << 1) + byte_value`
- Chunk boundary detected when: `hash & mask == 0`
- Configurable average chunk size via mask value
- Enforces minimum (64KB) and maximum (16MB) chunk sizes

**Benefits**:
- Same content always produces same chunks (deterministic)
- Inserting/deleting data only affects nearby chunks
- Better deduplication than fixed-size chunking

### 2. Storage Layer

**Directory Structure**:
```
.dedupe/
├── chunks/
│   ├── ab/
│   │   └── abc123...  (chunk data, named by SHA256 hash)
│   ├── de/
│   │   └── def456...
│   └── ...
└── metadata/
    ├── dir1/
    │   └── file1.txt.json  (file metadata)
    └── file2.txt.json
```

**Chunk Storage**:
- Each chunk stored once by its SHA256 hash
- Two-level directory structure (first 2 chars of hash) for scalability
- Duplicate chunks automatically deduplicated (same hash = same file)

**Metadata Storage**:
- JSON file per original file
- Contains: version, filename, size, modTime, chunk list, chunk size
- Mirrors original directory structure for easy navigation
- Version field allows format evolution

### 3. Metadata Caching

**Implementation** (`Fs.metaCache`):
```go
type Fs struct {
    metaMu    sync.RWMutex
    metaCache map[string]*FileMetadata
}
```

**Purpose**: Reduce API calls to underlying storage

**Behavior**:
- Read-through cache: load on first access, cache for subsequent reads
- Write-through cache: update cache and storage together
- Thread-safe using RWMutex

**Trade-offs**:
- Memory usage grows with number of files
- Stale cache possible if underlying storage modified externally
- Future: could add TTL or size-based eviction

### 4. File Reconstruction

**Implementation** (`chunkReader`):
- Implements `io.ReadCloser` interface
- Opens chunks sequentially as needed
- Streams data without loading entire file into memory

**Process**:
1. Client calls `Object.Open()`
2. `chunkReader` created with chunk list from metadata
3. Chunks opened lazily as `Read()` is called
4. Each chunk read to completion before moving to next
5. All chunks closed on `Close()`

## Data Flow

### Write Path (Upload)

```
File Input
    ↓
Content-Defined Chunking (Rabin)
    ↓
For each chunk:
    ├─ Compute SHA256 hash
    ├─ Check if chunk exists (by hash)
    └─ Store chunk if new
    ↓
Create metadata
    ├─ List of chunk hashes
    ├─ File size, modTime
    └─ Store as JSON
```

### Read Path (Download)

```
List/NewObject request
    ↓
Load metadata from storage
    ├─ Check cache first
    └─ Fetch from storage if missing
    ↓
Open() called
    ↓
Create chunkReader
    ↓
For each Read():
    ├─ Open next chunk if needed
    ├─ Read from current chunk
    └─ Move to next chunk on EOF
```

## Interface Implementations

### Fs Interface

- `List()`: Lists metadata files, returns virtual objects
- `NewObject()`: Loads metadata, creates Object
- `Put()`: Chunks data, stores chunks, saves metadata
- `Mkdir()`/`Rmdir()`: Operates on metadata directory
- `Hashes()`: Returns SHA256 support

### Object Interface

- `Open()`: Returns chunkReader for reassembly
- `Update()`: Re-chunks and stores data
- `Remove()`: Deletes metadata (chunks remain for GC)
- `Hash()`: Returns hash of concatenated chunk hashes
- `ModTime()`/`SetModTime()`: Stored in metadata

## Design Decisions

### Why Rabin Fingerprinting?

- **Deterministic**: Same content = same chunks across runs
- **Shift-resistant**: Inserting data doesn't re-chunk entire file
- **Industry standard**: Used by rsync, duplicity, restic, etc.

### Why SHA256 for Chunk Names?

- **Collision resistance**: Extremely low probability of hash collision
- **Standard**: Widely supported, well-tested
- **Performance**: Fast on modern hardware
- **Security**: Provides integrity checking
- **Optional verification**: `verify_hash` option enables bit-for-bit comparison for extra safety

### Why Separate Metadata Storage?

- **Fast listing**: Don't need to reassemble files to list directory
- **Atomic updates**: Metadata updated atomically
- **Cache-friendly**: Small JSON files easy to cache
- **Flexibility**: Can change metadata format without moving chunks

### Why No Garbage Collection?

- **Simplicity**: Keeps initial implementation focused
- **Safety**: Never delete data that might be needed
- **Future work**: Can add GC as separate command/feature
- **Workaround**: Manual cleanup of orphaned chunks possible

## Performance Characteristics

### Time Complexity

- **Upload**: O(n) where n = file size (chunking + hashing)
- **Download**: O(m) where m = number of chunks (sequential read)
- **List**: O(k) where k = number of files (metadata reads)

### Space Complexity

- **Dedup ratio**: Depends on content similarity (best case: near-zero for duplicates)
- **Metadata overhead**: ~200 bytes + (32 bytes × chunk count) per file
- **Cache memory**: O(f) where f = number of files accessed

### I/O Patterns

- **Upload**: Burst writes (all unique chunks written once)
- **Download**: Sequential reads (chunks read in order)
- **List**: Many small reads (metadata files)

## Future Enhancements

1. **Garbage Collection**: Identify and remove unused chunks
2. **Compression**: Compress chunks before storage
3. **Reed-Solomon**: Add redundancy for reliability
4. **Index Files**: Store chunk→file mapping for faster GC
5. **Concurrent Chunking**: Parallel processing for large files
6. **Smart Caching**: LRU eviction, TTL, size limits
7. **Encryption**: Per-chunk encryption (already possible via crypt backend)

## Testing Strategy

### Unit Tests
- Rabin chunker correctness
- Deterministic chunking
- Edge cases (empty, small files)
- Configuration validation

### Integration Tests
- Full upload/download cycle
- Multiple files with shared chunks
- Directory operations
- Error handling

### Performance Tests (Future)
- Large file handling
- Dedup ratio measurement
- Cache effectiveness
- Concurrent operations

## References

- Rabin Fingerprinting: "Fingerprinting by Random Polynomials" (1981)
- Content-Defined Chunking: Used by rsync, casync, restic, duplicity
- Similar implementations: rclone chunker backend, cache backend
