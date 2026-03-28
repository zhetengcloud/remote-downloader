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

	// UploadPart uploads a single part. partNumber starts from 1.
	// Returns ETag (or equivalent part identifier) for completing the upload.
	UploadPart(ctx context.Context, uploadID string, partNumber int, content io.Reader) (etag string, err error)

	// CompleteMultipartUpload finalizes the multipart upload with all parts.
	CompleteMultipartUpload(ctx context.Context, uploadID string, parts []PartInfo) error

	// AbortMultipartUpload cancels and cleans up the multipart upload on failure.
	AbortMultipartUpload(ctx context.Context, uploadID string) error
}

// PartInfo represents a successfully uploaded part for completing multipart upload.
type PartInfo struct {
	PartNumber int
	ETag       string
}
