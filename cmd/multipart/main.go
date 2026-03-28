package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aliyun/fc-runtime-go-sdk/fc"
	"go.zheteng.cloud/downloader/pkg/httpget"
	"go.zheteng.cloud/downloader/pkg/storage"
	"go.zheteng.cloud/downloader/pkg/storage/oss"
)

// Event represents the function input event.
type Event struct {
	URL          string `json:"url"`
	Bucket       string `json:"bucket"`
	ObjectKey    string `json:"objectKey"`
	BucketRegion string `json:"bucketRegion"`
	PoolSize     int    `json:"poolSize,omitempty"` // default: 3
	ChunkMB      int64  `json:"chunkmb,omitempty"`  // default: 5MB
}

// Result represents the function output.
type Result struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ObjectKey string `json:"objectKey"`
	Parts     int    `json:"parts"`
	Bytes     int64  `json:"bytes"`
}

// HandleRequest handles the FC event.
func HandleRequest(ctx context.Context, event Event) (Result, error) {
	// Set defaults
	poolSize := event.PoolSize
	mb := event.ChunkMB
	if mb < 1 {
		mb = 10
	}
	chunkSize := mb * 1024 * 1024

	accessKeyID := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	securityToken := os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN")

	cfg := oss.Config{
		Region:          event.BucketRegion,
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
		Bucket:          event.Bucket,
		SecurityToken:   securityToken,
	}

	progressCh := make(chan storage.Progress, 100)
	go func() {
		for p := range progressCh {
			log.Printf("progress: %s part=%d uploaded=%d status=%s",
				p.ObjectKey, p.PartNumber, p.Uploaded, p.Status)
		}
	}()

	store, err := oss.New(cfg, progressCh)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("create oss client: %v", err)}, nil
	}

	d := httpget.NewDownloader()
	totalSize, err := d.GetFileSize(ctx, event.URL)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("get file size: %v", err)}, nil
	}
	log.Printf("file size: %d bytes", totalSize)

	rangeCh, err := d.RangeDownload(ctx, event.URL, poolSize, chunkSize)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("range download: %v", err)}, nil
	}
	// Create multipart upload
	uploadID, err := store.CreateMultipartUpload(ctx, event.ObjectKey)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("create multipart upload: %v", err)}, nil
	}
	log.Printf("multipart upload created: %s", uploadID)

	// Upload directly from rangeCh - no adapter needed!
	parts, err := store.Upload(ctx, event.ObjectKey, uploadID, rangeCh, poolSize)
	if err != nil {
		_ = store.AbortMultipartUpload(ctx, uploadID, event.ObjectKey)
		return Result{Success: false, Message: fmt.Sprintf("upload: %v", err)}, nil
	}
	log.Printf("uploaded %d parts", len(parts))

	// Complete
	err = store.CompleteMultipartUpload(ctx, uploadID, event.ObjectKey, parts)
	if err != nil {
		_ = store.AbortMultipartUpload(ctx, uploadID, event.ObjectKey)
		return Result{Success: false, Message: fmt.Sprintf("complete multipart upload: %v", err)}, nil
	}

	log.Printf("download completed: oss://%s/%s", event.Bucket, event.ObjectKey)

	return Result{
		Success:   true,
		Message:   "upload completed",
		ObjectKey: event.ObjectKey,
		Parts:     len(parts),
		Bytes:     totalSize,
	}, nil
}

func main() {
	fc.Start(HandleRequest)
}
