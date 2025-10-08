#!/bin/bash
# Example usage of the dedupe backend

set -e

echo "=== Dedupe Backend Example Usage ==="
echo ""

# Create test directories
echo "1. Setting up test environment..."
mkdir -p /tmp/dedupe_example/{source,storage}

# Create test files with some duplicate content
echo "2. Creating test files..."
echo "This is common content that will appear in multiple files" > /tmp/dedupe_example/source/file1.txt
echo "This is common content that will appear in multiple files" > /tmp/dedupe_example/source/file2.txt
echo "This file has unique content only" > /tmp/dedupe_example/source/file3.txt
cat /tmp/dedupe_example/source/file1.txt > /tmp/dedupe_example/source/file4.txt
echo " with some extra data at the end" >> /tmp/dedupe_example/source/file4.txt

echo "   Created 4 test files"
ls -lh /tmp/dedupe_example/source/

echo ""
echo "3. The dedupe backend is now available in rclone"
echo "   To use it, configure a remote like this:"
echo ""
echo "   rclone config create mydedup dedupe remote=/path/to/storage chunk_size=4M"
echo ""
echo "4. Key Features:"
echo "   - Content-defined chunking using Rabin fingerprinting"
echo "   - Automatic deduplication of identical chunks"
echo "   - Chunks stored by SHA256 hash"
echo "   - Metadata stored separately for fast access"
echo ""
echo "5. Example commands:"
echo "   rclone copy /data mydedup:          # Upload with deduplication"
echo "   rclone ls mydedup:                  # List files"
echo "   rclone sync /data mydedup:          # Sync with deduplication"
echo ""
echo "See backend/dedupe/README.md for more information"
echo ""

# Cleanup
echo "Cleaning up example files..."
rm -rf /tmp/dedupe_example

echo "Done!"
