// Package dedupe provides a backend that deduplicates files using content-defined chunking
package dedupe

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	fshash "github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/lib/kv"
)

const (
	// Default chunk size for content-defined chunking
	defaultChunkSize = 4 * 1024 * 1024 // 4 MiB

	// Minimum and maximum chunk sizes
	minChunkSize = 64 * 1024       // 64 KiB
	maxChunkSize = 16 * 1024 * 1024 // 16 MiB

	// Rabin polynomial for rolling hash
	rabinPolynomial = 0x3DA3358B4DC173

	// Window size for rolling hash
	windowSize = 48

	// Directory names in the underlying storage
	chunksDir   = ".dedupe/chunks"
	metadataDir = ".dedupe/metadata"

	// Metadata format version
	metadataVersion = 1
)

var (
	// ErrInvalidChunkSize indicates an invalid chunk size configuration
	ErrInvalidChunkSize = errors.New("invalid chunk size")
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "dedupe",
		Description: "Deduplicate files using content-defined chunking",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Options: []fs.Option{{
			Name:     "remote",
			Required: true,
			Help: `Remote to store deduplicated data.

Normally should contain a ':' and a path, e.g. "myremote:path/to/dir",
"myremote:bucket" or maybe "myremote:" (not recommended).`,
		}, {
			Name:     "chunk_size",
			Advanced: false,
			Default:  fs.SizeSuffix(defaultChunkSize),
			Help: `Target chunk size for content-defined chunking.

Actual chunk sizes will vary around this target size.`,
		}, {
			Name:     "hash_type",
			Advanced: false,
			Default:  "sha256",
			Help:     `Hash type for chunk naming.`,
			Examples: []fs.OptionExample{{
				Value: "sha256",
				Help:  "SHA256 for chunk naming (recommended)",
			}},
		}, {
			Name:     "verify_hash",
			Advanced: true,
			Default:  false,
			Help: `Perform bit-for-bit comparison when chunk hash matches.

When enabled, if a chunk with the same hash already exists, the new chunk 
data will be compared byte-by-byte with the existing chunk to verify they 
are truly identical. This adds overhead but provides extra data integrity 
checking against hash collisions.`,
		}, {
			Name:     "store_full_hash",
			Advanced: false,
			Default:  true,
			Help: `Store hash of the complete file in metadata.

When enabled, the hash of the entire file is calculated and stored in the 
metadata. This allows the backend to provide the file hash immediately 
without having to read and reconstruct the file from chunks.`,
		}, {
			Name:     "use_chunk_cache",
			Advanced: true,
			Default:  true,
			Help: `Use local persistent cache for chunk hashes.

When enabled, maintains a local database of known chunk hashes to avoid
checking the underlying storage for every chunk. Significantly improves
upload performance for files with many chunks.`,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Remote        string        `config:"remote"`
	ChunkSize     fs.SizeSuffix `config:"chunk_size"`
	HashType      string        `config:"hash_type"`
	VerifyHash    bool          `config:"verify_hash"`
	StoreFullHash bool          `config:"store_full_hash"`
	UseChunkCache bool          `config:"use_chunk_cache"`
}

// Fs represents a dedupe filesystem
type Fs struct {
	name     string      // Name of this remote
	root     string      // Root path
	opt      Options     // Parsed options
	features *fs.Features // Optional features
	base     fs.Fs       // Underlying remote
	
	// Metadata cache
	metaMu    sync.RWMutex
	metaCache map[string]*FileMetadata
	
	// Chunk hash cache (persistent local database)
	chunkDB *kv.DB
}

// FileMetadata stores metadata about a file's chunks
type FileMetadata struct {
	Version   int                `json:"version"`
	Name      string             `json:"name"`
	Size      int64              `json:"size"`
	ModTime   time.Time          `json:"modTime"`
	Chunks    []string           `json:"chunks"` // List of chunk hashes
	ChunkSize int64              `json:"chunkSize"`
	Hash      string             `json:"hash,omitempty"`  // SHA256 hash of complete file (deprecated, use Hashes)
	Hashes    map[string]string  `json:"hashes,omitempty"` // Multiple hashes of complete file (e.g., "md5": "...", "sha1": "...", "sha256": "...")
}

// Object represents a dedupe object
type Object struct {
	fs       *Fs
	remote   string
	size     int64
	modTime  time.Time
	metadata *FileMetadata
}

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, rpath string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Validate chunk size
	if opt.ChunkSize < fs.SizeSuffix(minChunkSize) || opt.ChunkSize > fs.SizeSuffix(maxChunkSize) {
		return nil, ErrInvalidChunkSize
	}

	// Parse the remote path
	baseRemote, basePath, err := fspath.SplitFs(opt.Remote)
	if err != nil {
		return nil, fmt.Errorf("failed to parse remote %q: %w", opt.Remote, err)
	}

	// Combine base path with root
	if basePath != "" {
		basePath = path.Join(basePath, rpath)
	} else {
		basePath = rpath
	}

	// Create the base remote
	baseFs, err := cache.Get(ctx, baseRemote+basePath)
	if err != nil && err != fs.ErrorIsFile {
		return nil, fmt.Errorf("failed to create base remote: %w", err)
	}

	f := &Fs{
		name:      name,
		root:      rpath,
		opt:       *opt,
		base:      baseFs,
		metaCache: make(map[string]*FileMetadata),
	}

	// Initialize chunk hash cache if enabled
	if opt.UseChunkCache && kv.Supported() {
		db, err := kv.Start(ctx, "dedupe-chunks", baseFs)
		if err != nil {
			fs.Logf(f, "Failed to start chunk cache database: %v (continuing without cache)", err)
		} else {
			f.chunkDB = db
			fs.Debugf(f, "Chunk hash cache enabled at: %s", db.Path())
		}
	}

	// Set up features
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		DuplicateFiles:          false,
		ReadMimeType:            false,
		WriteMimeType:           false,
	}).Fill(ctx, f)

	return f, nil
}

// Name returns the name of the remote
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root of the remote
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("dedupe root '%s'", f.root)
}

// Precision returns the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return f.base.Precision()
}

// Hashes returns the supported hash sets
func (f *Fs) Hashes() fshash.Set {
	// Support SHA1 and SHA256 by default when store_full_hash is enabled
	if f.opt.StoreFullHash {
		return fshash.NewHashSet(fshash.MD5, fshash.SHA1, fshash.SHA256)
	}
	// Without stored hashes, we can only provide SHA256 from chunk hashes
	return fshash.NewHashSet(fshash.SHA256)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// List lists the objects and directories in dir
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// List metadata files to discover files
	metaPath := path.Join(metadataDir, dir)
	
	metaEntries, err := f.base.List(ctx, metaPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range metaEntries {
		switch e := entry.(type) {
		case fs.Object:
			// This is a metadata file, create a virtual object
			name := strings.TrimSuffix(e.Remote(), ".json")
			name = strings.TrimPrefix(name, metadataDir+"/")
			
			obj, err := f.NewObject(ctx, name)
			if err == nil {
				entries = append(entries, obj)
			}
		case fs.Directory:
			// Pass through directories
			dirName := strings.TrimPrefix(e.Remote(), metadataDir+"/")
			entries = append(entries, fs.NewDir(dirName, e.ModTime(ctx)))
		}
	}

	return entries, nil
}

// NewObject finds an object at the given remote path
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// Try to load metadata
	metadata, err := f.loadMetadata(ctx, remote)
	if err != nil {
		return nil, err
	}

	return &Object{
		fs:       f,
		remote:   remote,
		size:     metadata.Size,
		modTime:  metadata.ModTime,
		metadata: metadata,
	}, nil
}

// Put uploads a new object
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: src.Remote(),
		size:   src.Size(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir creates a directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	metaPath := path.Join(metadataDir, dir)
	return f.base.Mkdir(ctx, metaPath)
}

// Rmdir removes a directory
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	metaPath := path.Join(metadataDir, dir)
	return f.base.Rmdir(ctx, metaPath)
}

// loadMetadata loads metadata for a file from the underlying storage
func (f *Fs) loadMetadata(ctx context.Context, remote string) (*FileMetadata, error) {
	// Check cache first
	f.metaMu.RLock()
	if meta, ok := f.metaCache[remote]; ok {
		f.metaMu.RUnlock()
		return meta, nil
	}
	f.metaMu.RUnlock()

	// Load from storage
	metaPath := path.Join(metadataDir, remote+".json")
	metaObj, err := f.base.NewObject(ctx, metaPath)
	if err != nil {
		return nil, err
	}

	rc, err := metaObj.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var meta FileMetadata
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		return nil, err
	}

	// Cache it
	f.metaMu.Lock()
	f.metaCache[remote] = &meta
	f.metaMu.Unlock()

	return &meta, nil
}

// saveMetadata saves metadata for a file to the underlying storage
func (f *Fs) saveMetadata(ctx context.Context, remote string, meta *FileMetadata) error {
	// Update cache
	f.metaMu.Lock()
	f.metaCache[remote] = meta
	f.metaMu.Unlock()

	// Save to storage
	metaPath := path.Join(metadataDir, remote+".json")
	
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	reader := strings.NewReader(string(data))
	info := &metadataInfo{
		remote:  metaPath,
		size:    int64(len(data)),
		modTime: time.Now(),
	}

	_, err = f.base.Put(ctx, reader, info)
	return err
}

// Object interface implementation

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns a description of the Object
func (o *Object) String() string {
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification time
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size returns the size of the object
func (o *Object) Size() int64 {
	return o.size
}

// Hash returns the hash of the object
func (o *Object) Hash(ctx context.Context, ht fshash.Type) (string, error) {
	if o.metadata == nil {
		return "", errors.New("no metadata available")
	}
	
	// Try to get hash from metadata Hashes map first
	if o.metadata.Hashes != nil {
		hashName := ht.String()
		if hashVal, ok := o.metadata.Hashes[hashName]; ok && hashVal != "" {
			return hashVal, nil
		}
	}
	
	// Backward compatibility: check old Hash field for SHA256
	if ht == fshash.SHA256 && o.metadata.Hash != "" {
		return o.metadata.Hash, nil
	}
	
	// For SHA256, we can compute from chunk hashes as fallback
	if ht == fshash.SHA256 {
		h := sha256.New()
		for _, chunkHash := range o.metadata.Chunks {
			h.Write([]byte(chunkHash))
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	
	// Hash type not supported or not stored
	return "", fshash.ErrUnsupported
}

// SetModTime sets the modification time
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	o.modTime = t
	if o.metadata != nil {
		o.metadata.ModTime = t
		return o.fs.saveMetadata(ctx, o.remote, o.metadata)
	}
	return nil
}

// Storable returns whether the object is storable
func (o *Object) Storable() bool {
	return true
}

// Open opens the object for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.metadata == nil {
		return nil, errors.New("no metadata available")
	}

	// Create a reader that reconstructs the file from chunks
	return newChunkReader(ctx, o.fs, o.metadata), nil
}

// Update updates the object with new data
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// Chunk the input using content-defined chunking
	result, err := o.fs.chunkData(ctx, in)
	if err != nil {
		return err
	}

	// Create metadata
	o.metadata = &FileMetadata{
		Version:   metadataVersion,
		Name:      o.remote,
		Size:      src.Size(),
		ModTime:   src.ModTime(ctx),
		Chunks:    result.Chunks,
		ChunkSize: int64(o.fs.opt.ChunkSize),
		Hash:      result.FullHash,   // Backward compatibility
		Hashes:    result.FullHashes,  // New multi-hash support
	}
	o.size = src.Size()
	o.modTime = src.ModTime(ctx)

	// Save metadata
	return o.fs.saveMetadata(ctx, o.remote, o.metadata)
}

// Remove removes the object
func (o *Object) Remove(ctx context.Context) error {
	// Remove metadata
	metaPath := path.Join(metadataDir, o.remote+".json")
	metaObj, err := o.fs.base.NewObject(ctx, metaPath)
	if err != nil {
		return err
	}

	// Note: We don't remove chunks here as they might be shared
	// A separate garbage collection process would be needed
	return metaObj.Remove(ctx)
}

// chunkResult holds the result of chunking operation
type chunkResult struct {
	Chunks     []string
	FullHash   string            // Deprecated: use FullHashes
	FullHashes map[string]string // Multiple hashes of the complete file
}

// chunkData splits input data into content-defined chunks and stores them
func (f *Fs) chunkData(ctx context.Context, in io.Reader) (*chunkResult, error) {
	var chunks []string
	var hashers map[fshash.Type]hash.Hash
	
	// Initialize multiple file hashers if option is enabled
	if f.opt.StoreFullHash {
		hashers = map[fshash.Type]hash.Hash{
			fshash.MD5:    md5.New(),
			fshash.SHA1:   sha1.New(),
			fshash.SHA256: sha256.New(),
		}
	}
	
	// Use multiWriter to hash the full file with multiple algorithms while chunking
	var reader io.Reader = in
	if hashers != nil {
		writers := make([]io.Writer, 0, len(hashers))
		for _, h := range hashers {
			writers = append(writers, h)
		}
		multiWriter := io.MultiWriter(writers...)
		reader = io.TeeReader(in, multiWriter)
	}
	
	chunker := newRabinChunker(reader, int(f.opt.ChunkSize))

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Hash the chunk with SHA256 for storage naming
		h := sha256.New()
		h.Write(chunk)
		chunkHash := hex.EncodeToString(h.Sum(nil))

		// Store the chunk (if not already present)
		if err := f.storeChunk(ctx, chunkHash, chunk); err != nil {
			return nil, err
		}

		chunks = append(chunks, chunkHash)
	}

	result := &chunkResult{
		Chunks: chunks,
	}
	
	// Store full file hashes if enabled
	if hashers != nil {
		result.FullHashes = make(map[string]string)
		for ht, hasher := range hashers {
			hashValue := hex.EncodeToString(hasher.Sum(nil))
			result.FullHashes[ht.String()] = hashValue
			
			// Also set FullHash field for backward compatibility (SHA256)
			if ht == fshash.SHA256 {
				result.FullHash = hashValue
			}
		}
	}
	
	return result, nil
}

// storeChunk stores a chunk if it doesn't already exist
func (f *Fs) storeChunk(ctx context.Context, hash string, data []byte) error {
	chunkPath := path.Join(chunksDir, hash[:2], hash)

	// Check chunk cache first if enabled
	if f.chunkDB != nil {
		if f.chunkExistsInCache(ctx, hash) {
			fs.Debugf(hash, "Chunk found in cache, skipping upload")
			return nil
		}
	}

	// Check if chunk already exists in storage
	existingObj, err := f.base.NewObject(ctx, chunkPath)
	if err == nil {
		// Chunk already exists
		if f.opt.VerifyHash {
			// Perform bit-for-bit comparison
			if err := f.verifyChunkData(ctx, existingObj, data); err != nil {
				return fmt.Errorf("chunk verification failed for %s: %w", hash, err)
			}
		}
		// Add to cache for future use
		if f.chunkDB != nil {
			f.addChunkToCache(ctx, hash)
		}
		// Chunk exists and is verified (if requested), skip storing
		return nil
	}

	// Store new chunk
	reader := strings.NewReader(string(data))
	info := &metadataInfo{
		remote:  chunkPath,
		size:    int64(len(data)),
		modTime: time.Now(),
	}

	_, err = f.base.Put(ctx, reader, info)
	if err != nil {
		return err
	}

	// Add to cache after successful upload
	if f.chunkDB != nil {
		f.addChunkToCache(ctx, hash)
	}

	return nil
}

// verifyChunkData performs a bit-for-bit comparison between existing chunk and new data
func (f *Fs) verifyChunkData(ctx context.Context, existingObj fs.Object, newData []byte) error {
	// Open existing chunk
	rc, err := existingObj.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open existing chunk: %w", err)
	}
	defer rc.Close()

	// Read existing chunk data
	existingData, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read existing chunk: %w", err)
	}

	// Compare sizes first
	if len(existingData) != len(newData) {
		return fmt.Errorf("chunk size mismatch: existing %d bytes, new %d bytes", len(existingData), len(newData))
	}

	// Bit-for-bit comparison
	if !bytes.Equal(existingData, newData) {
		return errors.New("chunk data mismatch despite matching hash (possible hash collision)")
	}

	return nil
}

// chunkCacheOp represents a chunk cache database operation
type chunkCacheOp struct {
	key   string
	value []byte
}

func (op *chunkCacheOp) Do(ctx context.Context, b kv.Bucket) error {
	if op.value == nil {
		// Delete operation
		return b.Delete([]byte(op.key))
	}
	// Put operation
	return b.Put([]byte(op.key), op.value)
}

// chunkExistsInCache checks if a chunk hash exists in the local cache
func (f *Fs) chunkExistsInCache(ctx context.Context, hash string) bool {
	if f.chunkDB == nil {
		return false
	}

	var exists bool
	getter := &chunkCacheGetter{key: hash, exists: &exists}
	err := f.chunkDB.Do(false, getter)
	if err != nil {
		fs.Debugf(hash, "Failed to check chunk cache: %v", err)
		return false
	}
	return exists
}

// chunkCacheGetter checks if a key exists in the cache
type chunkCacheGetter struct {
	key    string
	exists *bool
}

func (op *chunkCacheGetter) Do(ctx context.Context, b kv.Bucket) error {
	val := b.Get([]byte(op.key))
	*op.exists = val != nil
	return nil
}

// addChunkToCache adds a chunk hash to the local cache
func (f *Fs) addChunkToCache(ctx context.Context, hash string) {
	if f.chunkDB == nil {
		return
	}

	// Store a simple marker (timestamp) to indicate chunk exists
	timestamp := []byte(time.Now().Format(time.RFC3339))
	op := &chunkCacheOp{key: hash, value: timestamp}
	err := f.chunkDB.Do(true, op)
	if err != nil {
		fs.Debugf(hash, "Failed to add chunk to cache: %v", err)
	}
}

// removeChunkFromCache removes a chunk hash from the local cache
func (f *Fs) removeChunkFromCache(ctx context.Context, hash string) {
	if f.chunkDB == nil {
		return
	}

	op := &chunkCacheOp{key: hash, value: nil}
	err := f.chunkDB.Do(true, op)
	if err != nil {
		fs.Debugf(hash, "Failed to remove chunk from cache: %v", err)
	}
}

// rabinChunker implements content-defined chunking using Rabin fingerprinting
type rabinChunker struct {
	reader      io.Reader
	targetSize  int
	window      []byte
	hash        uint64
	buffer      []byte
	polynomial  uint64
	windowMask  uint64
}

func newRabinChunker(r io.Reader, targetSize int) *rabinChunker {
	return &rabinChunker{
		reader:     r,
		targetSize: targetSize,
		window:     make([]byte, 0, windowSize),
		buffer:     make([]byte, 0, maxChunkSize),
		polynomial: rabinPolynomial,
		windowMask: (1 << 13) - 1, // For ~8KB average chunks
	}
}

func (rc *rabinChunker) Next() ([]byte, error) {
	rc.buffer = rc.buffer[:0]
	rc.hash = 0
	rc.window = rc.window[:0]

	buf := make([]byte, 8192)
	for {
		n, err := rc.reader.Read(buf)
		if n > 0 {
			for i := 0; i < n; i++ {
				b := buf[i]
				rc.buffer = append(rc.buffer, b)

				// Update rolling hash
				rc.hash = (rc.hash << 1) + uint64(b)
				
				// Maintain window
				if len(rc.window) >= windowSize {
					rc.window = rc.window[1:]
				}
				rc.window = append(rc.window, b)

				// Check for chunk boundary
				if len(rc.buffer) >= minChunkSize {
					if (rc.hash&rc.windowMask) == 0 || len(rc.buffer) >= maxChunkSize {
						return rc.buffer, nil
					}
				}
			}
		}

		if err == io.EOF {
			if len(rc.buffer) > 0 {
				return rc.buffer, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
	}
}

// chunkReader reconstructs a file from its chunks
type chunkReader struct {
	ctx      context.Context
	fs       *Fs
	metadata *FileMetadata
	chunks   []io.ReadCloser
	current  int
	closed   bool
}

func newChunkReader(ctx context.Context, f *Fs, meta *FileMetadata) *chunkReader {
	return &chunkReader{
		ctx:      ctx,
		fs:       f,
		metadata: meta,
		chunks:   make([]io.ReadCloser, 0),
		current:  0,
	}
}

func (cr *chunkReader) Read(p []byte) (n int, err error) {
	if cr.closed {
		return 0, errors.New("reader closed")
	}

	for {
		// Open next chunk if needed
		if cr.current >= len(cr.metadata.Chunks) {
			return n, io.EOF
		}

		// Get current chunk reader
		if cr.current >= len(cr.chunks) {
			// Open this chunk
			chunkHash := cr.metadata.Chunks[cr.current]
			chunkPath := path.Join(chunksDir, chunkHash[:2], chunkHash)
			
			chunkObj, err := cr.fs.base.NewObject(cr.ctx, chunkPath)
			if err != nil {
				return n, err
			}

			rc, err := chunkObj.Open(cr.ctx)
			if err != nil {
				return n, err
			}
			cr.chunks = append(cr.chunks, rc)
		}

		// Read from current chunk
		nn, err := cr.chunks[cr.current].Read(p[n:])
		n += nn

		if err == io.EOF {
			// Close this chunk and move to next
			cr.chunks[cr.current].Close()
			cr.current++
			
			if n > 0 {
				// We read some data, return it
				return n, nil
			}
			// Continue to next chunk
			continue
		}

		if err != nil {
			return n, err
		}

		if n > 0 {
			return n, nil
		}
	}
}

func (cr *chunkReader) Close() error {
	if cr.closed {
		return nil
	}
	cr.closed = true

	for _, rc := range cr.chunks {
		rc.Close()
	}
	return nil
}

// metadataInfo implements fs.ObjectInfo for metadata objects
type metadataInfo struct {
	remote  string
	size    int64
	modTime time.Time
}

func (mi *metadataInfo) Fs() fs.Info               { return nil }
func (mi *metadataInfo) Remote() string            { return mi.remote }
func (mi *metadataInfo) String() string            { return mi.remote }
func (mi *metadataInfo) ModTime(ctx context.Context) time.Time { return mi.modTime }
func (mi *metadataInfo) Size() int64               { return mi.size }
func (mi *metadataInfo) Storable() bool            { return true }
func (mi *metadataInfo) Hash(ctx context.Context, ht fshash.Type) (string, error) {
	return "", fshash.ErrUnsupported
}

// Check interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)

// commandHelp describes the available backend commands
var commandHelp = []fs.CommandHelp{{
	Name:  "gc",
	Short: "Run garbage collection on orphaned chunks",
	Long: `This command scans all metadata files and identifies chunks that are no longer
referenced by any file. Orphaned chunks are then deleted from storage.

Usage:
    rclone backend gc dedupe:

Options:
    --dry-run: Show what would be deleted without actually deleting
    --sync-cache: Synchronize chunk cache with actual storage (removes stale entries)
`,
	Opts: map[string]string{
		"dry-run":    "Set to true to show what would be deleted without deleting",
		"sync-cache": "Set to true to also clean up the chunk cache",
	},
}}

// Command runs a backend command
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out any, err error) {
	switch name {
	case "gc":
		dryRun := opt["dry-run"] == "true"
		syncCache := opt["sync-cache"] == "true"
		stats, err := f.garbageCollect(ctx, dryRun, syncCache)
		if err != nil {
			return nil, err
		}
		return stats, nil
	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// GCStats holds statistics from garbage collection
type GCStats struct {
	MetadataFiles    int      `json:"metadataFiles"`
	ReferencedChunks int      `json:"referencedChunks"`
	TotalChunks      int      `json:"totalChunks"`
	OrphanedChunks   int      `json:"orphanedChunks"`
	DeletedChunks    int      `json:"deletedChunks"`
	CacheSynced      bool     `json:"cacheSynced,omitempty"`
	CacheEntriesBefore int    `json:"cacheEntriesBefore,omitempty"`
	CacheEntriesAfter  int    `json:"cacheEntriesAfter,omitempty"`
	Errors           []string `json:"errors,omitempty"`
}

// garbageCollect performs garbage collection on orphaned chunks
func (f *Fs) garbageCollect(ctx context.Context, dryRun, syncCache bool) (*GCStats, error) {
	stats := &GCStats{}
	
	// Step 1: Build set of all referenced chunks from metadata files
	referencedChunks := make(map[string]bool)
	
	fs.Infof(f, "Scanning metadata files...")
	err := walk.ListR(ctx, f.base, metadataDir, true, -1, walk.ListAll, func(entries fs.DirEntries) error {
		for _, entry := range entries {
			if o, ok := entry.(fs.Object); ok {
				stats.MetadataFiles++
				
				// Load metadata
				rc, err := o.Open(ctx)
				if err != nil {
					stats.Errors = append(stats.Errors, fmt.Sprintf("Failed to open %s: %v", o.Remote(), err))
					continue
				}
				
				var meta FileMetadata
				if err := json.NewDecoder(rc).Decode(&meta); err != nil {
					rc.Close()
					stats.Errors = append(stats.Errors, fmt.Sprintf("Failed to parse %s: %v", o.Remote(), err))
					continue
				}
				rc.Close()
				
				// Mark all chunks as referenced
				for _, chunkHash := range meta.Chunks {
					referencedChunks[chunkHash] = true
				}
			}
		}
		return nil
	})
	
	if err != nil {
		return stats, fmt.Errorf("failed to scan metadata: %w", err)
	}
	
	stats.ReferencedChunks = len(referencedChunks)
	fs.Infof(f, "Found %d referenced chunks from %d metadata files", stats.ReferencedChunks, stats.MetadataFiles)
	
	// Step 2: Scan all chunks and identify orphans
	fs.Infof(f, "Scanning chunk storage...")
	err = walk.ListR(ctx, f.base, chunksDir, true, -1, walk.ListAll, func(entries fs.DirEntries) error {
		for _, entry := range entries {
			if o, ok := entry.(fs.Object); ok {
				stats.TotalChunks++
				
				// Extract chunk hash from path (e.g., .dedupe/chunks/ab/abc123... -> abc123...)
				chunkPath := o.Remote()
				parts := strings.Split(chunkPath, "/")
				if len(parts) < 3 {
					continue
				}
				chunkHash := parts[len(parts)-1]
				
				// Check if this chunk is referenced
				if !referencedChunks[chunkHash] {
					stats.OrphanedChunks++
					
					if !dryRun {
						// Delete the orphaned chunk
						if err := o.Remove(ctx); err != nil {
							stats.Errors = append(stats.Errors, fmt.Sprintf("Failed to delete %s: %v", chunkPath, err))
						} else {
							stats.DeletedChunks++
							// Also remove from cache if present
							if f.chunkDB != nil {
								f.removeChunkFromCache(ctx, chunkHash)
							}
						}
					}
				} else {
					// Chunk is referenced, add to cache if not already there
					if f.chunkDB != nil && !dryRun {
						f.addChunkToCache(ctx, chunkHash)
					}
				}
			}
		}
		return nil
	})
	
	if err != nil {
		return stats, fmt.Errorf("failed to scan chunks: %w", err)
	}
	
	if dryRun {
		fs.Infof(f, "DRY RUN: Would delete %d orphaned chunks out of %d total chunks", stats.OrphanedChunks, stats.TotalChunks)
	} else {
		fs.Infof(f, "Deleted %d orphaned chunks out of %d total chunks", stats.DeletedChunks, stats.TotalChunks)
	}
	
	// Step 3: Optionally synchronize chunk cache
	if syncCache && f.chunkDB != nil && !dryRun {
		fs.Infof(f, "Synchronizing chunk cache...")
		err = f.syncChunkCache(ctx, referencedChunks, stats)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Sprintf("Cache sync error: %v", err))
		} else {
			stats.CacheSynced = true
			fs.Infof(f, "Cache synchronized: %d entries before, %d entries after", 
				stats.CacheEntriesBefore, stats.CacheEntriesAfter)
		}
	}
	
	return stats, nil
}

// syncChunkCache removes stale entries from the chunk cache
func (f *Fs) syncChunkCache(ctx context.Context, referencedChunks map[string]bool, stats *GCStats) error {
	if f.chunkDB == nil {
		return nil
	}

	// Count entries before
	op := &chunkCacheCounter{}
	if err := f.chunkDB.Do(false, op); err != nil {
		return err
	}
	stats.CacheEntriesBefore = op.count

	// Remove unreferenced chunks from cache
	removeOp := &chunkCacheSyncer{referenced: referencedChunks}
	if err := f.chunkDB.Do(true, removeOp); err != nil {
		return err
	}

	// Count entries after
	op = &chunkCacheCounter{}
	if err := f.chunkDB.Do(false, op); err != nil {
		return err
	}
	stats.CacheEntriesAfter = op.count

	return nil
}

// chunkCacheCounter counts entries in the cache
type chunkCacheCounter struct {
	count int
}

func (op *chunkCacheCounter) Do(ctx context.Context, b kv.Bucket) error {
	op.count = 0
	return b.ForEach(func(k, v []byte) error {
		op.count++
		return nil
	})
}

// chunkCacheSyncer removes unreferenced chunks from cache
type chunkCacheSyncer struct {
	referenced map[string]bool
}

func (op *chunkCacheSyncer) Do(ctx context.Context, b kv.Bucket) error {
	var toDelete []string
	
	// First, collect keys to delete
	err := b.ForEach(func(k, v []byte) error {
		key := string(k)
		if !op.referenced[key] {
			toDelete = append(toDelete, key)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Then delete them
	for _, key := range toDelete {
		if err := b.Delete([]byte(key)); err != nil {
			fs.Debugf(key, "Failed to remove from cache: %v", err)
		}
	}

	return nil
}
