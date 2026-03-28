// Package storage
package storage

import (
	"context"
	"io"
)

// Storage defines the interface for multipart upload to different cloud storage.
// Implementations: OSS, S3, or local file for testing.
type Storage interface {
	// CreateMultipartUpload initiates a multipart upload and returns an upload ID.
	CreateMultipartUpload(ctx context.Context, objectKey string) (uploadID string, err error)

	// CompleteMultipartUpload finalizes the multipart upload with all parts.
	CompleteMultipartUpload(ctx context.Context, uploadID string, objectKey string, parts []PartInfo) error

	// AbortMultipartUpload cancels and cleans up the multipart upload on failure.
	AbortMultipartUpload(ctx context.Context, uploadID string, objectKey string) error
}

// PartInfo represents a successfully uploaded part for completing multipart upload.
type PartInfo struct {
	PartNumber int
	ETag       string
}

// Progress represents the upload progress.
type Progress struct {
	ObjectKey  string // object key being uploaded
	UploadID   string // upload ID for multipart upload (optional)
	PartNumber int    // current part being uploaded (0 if not applicable)
	Uploaded   int64  // bytes uploaded so far
	Status     string // "uploading" | "completed" | "aborted" | "failed"
}

// RangeInfo represents a part to be uploaded.
// Implementations: convert httpget.Result or create custom sources.
type RangeInfo interface {
	PartNumber() int
	Body() io.ReadCloser
}
