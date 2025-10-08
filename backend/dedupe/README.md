# Dedupe Backend

The dedupe backend provides content-defined chunking and deduplication of files stored on an underlying remote.

## Features

- **Content-Defined Chunking**: Uses Rabin fingerprinting to split files into variable-size chunks based on content
- **Deduplication**: Stores each unique chunk only once, regardless of how many files contain it
- **Efficient Storage**: Chunks are stored using their SHA256 hash as the filename, enabling automatic deduplication
- **Metadata Caching**: Reduces calls to the underlying storage by caching file metadata

## How It Works

1. **Chunking**: Files are split into chunks using a rolling hash algorithm (Rabin fingerprinting). This ensures that:
   - The same content always produces the same chunks
   - Inserting/deleting data in the middle of a file only affects nearby chunks
   - Average chunk size is configurable (default 4 MiB)

2. **Storage Layout**: The backend uses a specific directory structure on the underlying remote:
   - `.dedupe/chunks/XX/HASH`: Stores chunk data, where XX is the first two characters of the hash
   - `.dedupe/metadata/PATH.json`: Stores file metadata including chunk list and file properties

3. **Deduplication**: When storing a chunk:
   - The chunk is hashed using SHA256
   - If a chunk with that hash already exists, it's reused
   - Only unique chunks are stored

## Configuration

```
rclone config create mydedup dedupe remote=myremote:path chunk_size=4M
```

### Options

- `remote`: The underlying remote to store data (required)
- `chunk_size`: Target size for chunks (default: 4M, min: 64K, max: 16M)
- `hash_type`: Hash algorithm for chunk naming (default: sha256)
- `store_full_hash`: Store hash of complete file in metadata (default: true)
- `verify_hash`: Perform bit-for-bit comparison when chunk hash matches (default: false, advanced)

The `store_full_hash` option (enabled by default) calculates and stores the hash of 
the entire file in the metadata during upload. This allows the backend to immediately 
provide the file hash when requested by upper layers, without having to read and 
reconstruct the file from chunks.

The `verify_hash` option adds extra data integrity checking. When enabled, if a chunk 
with the same hash already exists, the backend will read the existing chunk and compare 
it byte-by-byte with the new chunk data. This protects against the extremely unlikely 
case of hash collisions but adds I/O overhead.

## Use Cases

- **Backup Systems**: Store multiple versions of files efficiently
- **VM Images**: Store similar disk images with deduplication
- **Development**: Store multiple branches with shared files
- **Archival**: Long-term storage of evolving datasets

## Example Usage

```bash
# Create a dedupe remote
rclone config create mydedup dedupe remote=s3:mybucket/dedupe

# Copy files with automatic deduplication
rclone copy /local/files mydedup:

# List files (shows original file structure)
rclone ls mydedup:

# Sync files
rclone sync /local/files mydedup:
```

## Performance Considerations

- **First Upload**: Initial upload requires chunking and hashing all data
- **Subsequent Uploads**: Files with shared content benefit from deduplication
- **Read Performance**: Reading requires reassembling chunks (slight overhead)
- **Metadata Caching**: File metadata is cached in memory to reduce API calls

## Limitations

- **No Garbage Collection**: Deleted file chunks remain in storage (manual cleanup needed)
- **Metadata Size**: Each file requires a metadata object
- **Chunk Overhead**: Small files still create metadata overhead
- **No Encryption**: Data is stored as-is (combine with crypt backend if needed)

## Technical Details

### Rolling Hash Algorithm

The backend uses Rabin fingerprinting with:
- Polynomial: 0x3DA3358B4DC173
- Window size: 48 bytes
- Chunk boundaries detected when: `hash & mask == 0`
- Minimum chunk size: 64 KiB
- Maximum chunk size: 16 MiB

### Metadata Format

File metadata is stored as JSON:
```json
{
  "version": 1,
  "name": "path/to/file.txt",
  "size": 1234567,
  "modTime": "2024-01-01T12:00:00Z",
  "chunks": [
    "abc123...",
    "def456..."
  ],
  "chunkSize": 4194304,
  "hash": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

The `version` field allows for future metadata format changes while maintaining 
backward compatibility. The `hash` field (when `store_full_hash` is enabled) 
contains the SHA256 hash of the complete file, allowing fast hash retrieval 
without reconstructing the file from chunks.

## Combining with Other Backends

The dedupe backend can wrap any other backend:

```bash
# Dedupe over encrypted storage
rclone config create mydedup dedupe remote=mycrypt:

# Dedupe over cloud storage
rclone config create mydedup dedupe remote=s3:mybucket/dedup
```

## Standard Options

All standard rclone options apply to dedupe backends.
