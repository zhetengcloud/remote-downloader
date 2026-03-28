// Package oss
package oss

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"go.zheteng.cloud/downloader/pkg/storage"
)

// OSS implements storage.Storage for Aliyun OSS using SDK v2.
type OSS struct {
	client *oss.Client
	bucket string

	progressCh chan<- storage.Progress
}

// Config holds OSS configuration.
type Config struct {
	Region          string
	Endpoint        string // optional, will use default if empty
	AccessKeyID     string
	AccessKeySecret string
	Bucket          string
}

// New creates a new OSS storage with SDK v2.
func New(cfg Config, progressCh chan<- storage.Progress) (*OSS, error) {
	var creds credentials.CredentialsProvider
	if cfg.AccessKeyID != "" && cfg.AccessKeySecret != "" {
		creds = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.AccessKeySecret, "")
	} else {
		// Try environment variables
		creds = credentials.NewEnvironmentVariableCredentialsProvider()
	}

	loadCfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(creds).
		WithRegion(cfg.Region)

	if cfg.Endpoint != "" {
		loadCfg.WithEndpoint(cfg.Endpoint)
	}

	client := oss.NewClient(loadCfg)

	return &OSS{
		client:     client,
		bucket:     cfg.Bucket,
		progressCh: progressCh,
	}, nil
}

// CreateMultipartUpload initiates a multipart upload.
func (o *OSS) CreateMultipartUpload(ctx context.Context, objectKey string) (string, error) {
	req := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(o.bucket),
		Key:    oss.Ptr(objectKey),
	}

	resp, err := o.client.InitiateMultipartUpload(ctx, req)
	if err != nil {
		return "", fmt.Errorf("initiate multipart upload: %w", err)
	}

	return oss.ToString(resp.UploadId), nil
}

// Upload uploads parts from the range channel concurrently.
// It spawns poolSize workers to upload parts in parallel.
// Returns PartInfo slice on success, error on failure.
func (o *OSS) Upload(ctx context.Context, objectKey, uploadID string, rangeCh <-chan storage.RangeInfo, poolSize int) ([]storage.PartInfo, error) {
	if poolSize <= 0 {
		poolSize = 3
	}

	// Channels for collecting results - no locks needed
	chParts := make(chan storage.PartInfo, poolSize)
	chErr := make(chan error, 1) // Buffered to not block on first error

	var wg sync.WaitGroup
	wg.Add(poolSize)

	// Start workers
	for range poolSize {
		go func() {
			defer wg.Done()
			for info := range rangeCh {
				select {
				case <-ctx.Done():
					_ = info.Body().Close()
					return
				default:
				}

				etag, err := o.uploadPart(ctx, objectKey, uploadID, info.PartNumber(), info.Body())
				_ = info.Body().Close()

				if err != nil {
					// Send first error only (non-blocking)
					select {
					case chErr <- err:
					default:
					}
					continue
				}

				chParts <- storage.PartInfo{
					PartNumber: info.PartNumber(),
					ETag:       etag,
				}
			}
		}()
	}

	// Close chParts when all workers done (chErr stays open)
	go func() {
		wg.Wait()
		close(chParts)
	}()

	// Collect results using maps (no append, no locks)
	parts := make(map[int]storage.PartInfo)
	var someErr error

	for {
		select {
		case err := <-chErr:
			someErr = err
		case p, ok := <-chParts:
			if !ok {
				// chParts closed, we're done
				if someErr != nil {
					return nil, someErr
				}
				// Convert map to slice
				return mapToSlice(parts), nil
			}
			parts[p.PartNumber] = p
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// mapToSlice converts parts map to sorted slice.
func mapToSlice(parts map[int]storage.PartInfo) []storage.PartInfo {
	result := make([]storage.PartInfo, 0, len(parts))
	for _, p := range parts {
		result = append(result, p)
	}
	// Sort by part number (OSS requires ascending order)
	sort.Slice(result, func(i, j int) bool {
		return result[i].PartNumber < result[j].PartNumber
	})
	return result
}

// uploadPart uploads a single part with progress reporting.
func (o *OSS) uploadPart(ctx context.Context, objectKey, uploadID string, partNumber int, content io.Reader) (string, error) {
	req := &oss.UploadPartRequest{
		Bucket:     oss.Ptr(o.bucket),
		Key:        oss.Ptr(objectKey),
		UploadId:   oss.Ptr(uploadID),
		PartNumber: int32(partNumber),
		Body:       content,
		ProgressFn: func(increment, transferred, total int64) {
			o.reportProgress(objectKey, partNumber, transferred, "uploading")
		},
	}

	resp, err := o.client.UploadPart(ctx, req)
	if err != nil {
		return "", fmt.Errorf("upload part %d: %w", partNumber, err)
	}

	return oss.ToString(resp.ETag), nil
}

// CompleteMultipartUpload finalizes the multipart upload.
func (o *OSS) CompleteMultipartUpload(ctx context.Context, uploadID string, objectKey string, parts []storage.PartInfo) error {
	// Convert parts to OSS format
	ossParts := make([]oss.UploadPart, len(parts))
	for i, p := range parts {
		ossParts[i] = oss.UploadPart{
			PartNumber: int32(p.PartNumber),
			ETag:       oss.Ptr(p.ETag),
		}
	}

	req := &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(o.bucket),
		Key:      oss.Ptr(objectKey),
		UploadId: oss.Ptr(uploadID),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: ossParts,
		},
	}

	_, err := o.client.CompleteMultipartUpload(ctx, req)
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}

	o.reportProgress(objectKey, 0, 0, "completed")

	return nil
}

// AbortMultipartUpload cancels the multipart upload.
func (o *OSS) AbortMultipartUpload(ctx context.Context, uploadID string, objectKey string) error {
	req := &oss.AbortMultipartUploadRequest{
		Bucket:   oss.Ptr(o.bucket),
		Key:      oss.Ptr(objectKey),
		UploadId: oss.Ptr(uploadID),
	}

	_, err := o.client.AbortMultipartUpload(ctx, req)
	if err != nil {
		return fmt.Errorf("abort multipart upload: %w", err)
	}

	o.reportProgress(objectKey, 0, 0, "aborted")

	return nil
}

// reportProgress sends progress update if channel is configured.
func (o *OSS) reportProgress(objectKey string, partNumber int, uploaded int64, status string) {
	if o.progressCh != nil {
		select {
		case o.progressCh <- storage.Progress{
			ObjectKey:  objectKey,
			PartNumber: partNumber,
			Uploaded:   uploaded,
			Status:     status,
		}:
		default:
		}
	}
}
