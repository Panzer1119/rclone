// Package dedupe provides a backend that deduplicates files using content-defined chunking
package dedupe

import (
	"bytes"
	"context"
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
}

// FileMetadata stores metadata about a file's chunks
type FileMetadata struct {
	Version   int       `json:"version"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"modTime"`
	Chunks    []string  `json:"chunks"` // List of chunk hashes
	ChunkSize int64     `json:"chunkSize"`
	Hash      string    `json:"hash,omitempty"` // Hash of complete file (optional)
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
	if ht != fshash.SHA256 {
		return "", fshash.ErrUnsupported
	}
	
	if o.metadata == nil {
		return "", errors.New("no metadata available")
	}
	
	// Return stored full file hash if available
	if o.metadata.Hash != "" {
		return o.metadata.Hash, nil
	}
	
	// Fallback: compute hash from chunk hashes
	h := sha256.New()
	for _, chunkHash := range o.metadata.Chunks {
		h.Write([]byte(chunkHash))
	}
	
	return hex.EncodeToString(h.Sum(nil)), nil
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
		Hash:      result.FullHash,
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
	Chunks   []string
	FullHash string
}

// chunkData splits input data into content-defined chunks and stores them
func (f *Fs) chunkData(ctx context.Context, in io.Reader) (*chunkResult, error) {
	var chunks []string
	var fullHasher hash.Hash
	
	// Initialize full file hasher if option is enabled
	if f.opt.StoreFullHash {
		fullHasher = sha256.New()
	}
	
	// Use TeeReader to hash the full file while chunking
	var reader io.Reader = in
	if fullHasher != nil {
		reader = io.TeeReader(in, fullHasher)
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

		// Hash the chunk
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
	
	// Store full file hash if enabled
	if fullHasher != nil {
		result.FullHash = hex.EncodeToString(fullHasher.Sum(nil))
	}

	return result, nil
}

// storeChunk stores a chunk if it doesn't already exist
func (f *Fs) storeChunk(ctx context.Context, hash string, data []byte) error {
	chunkPath := path.Join(chunksDir, hash[:2], hash)

	// Check if chunk already exists
	existingObj, err := f.base.NewObject(ctx, chunkPath)
	if err == nil {
		// Chunk already exists
		if f.opt.VerifyHash {
			// Perform bit-for-bit comparison
			if err := f.verifyChunkData(ctx, existingObj, data); err != nil {
				return fmt.Errorf("chunk verification failed for %s: %w", hash, err)
			}
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
	return err
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
