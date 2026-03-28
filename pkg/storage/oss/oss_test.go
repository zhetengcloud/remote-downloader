package oss

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"go.zheteng.cloud/downloader/pkg/httpget"
	"go.zheteng.cloud/downloader/pkg/storage"
)

func init() {
	_ = godotenv.Load(".env.test")
}

const testURL = "https://mirrors.aliyun.com/gradle/gradle-1.0-all.zip"

func TestOSS_FullFlow(t *testing.T) {
	cfg := Config{
		Region:          os.Getenv("OSS_REGION"),
		AccessKeyID:     os.Getenv("OSS_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("OSS_ACCESS_KEY_SECRET"),
		Bucket:          os.Getenv("OSS_BUCKET"),
	}

	progressCh := make(chan storage.Progress, 100)
	go func() {
		for p := range progressCh {
			t.Logf("progress: %s part=%d uploaded=%d status=%s",
				p.ObjectKey, p.PartNumber, p.Uploaded, p.Status)
		}
	}()

	store, err := New(cfg, progressCh)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	objectKey := "gradle-1.0-all.zip"

	uploadID, err := store.CreateMultipartUpload(ctx, objectKey)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	t.Logf("upload ID: %s", uploadID)

	d := httpget.NewDownloader()
	poolSize := 2
	chunkSize := int64(1024 * 1024) // 1MB chunks for testing

	// RangeDownload now returns storage.RangeInfo directly!
	rangeCh, err := d.RangeDownload(ctx, testURL, poolSize, chunkSize)
	if err != nil {
		_ = store.AbortMultipartUpload(ctx, uploadID, objectKey)
		t.Fatalf("RangeDownload failed: %v", err)
	}

	parts, err := store.Upload(ctx, objectKey, uploadID, rangeCh, poolSize)
	if err != nil {
		_ = store.AbortMultipartUpload(ctx, uploadID, objectKey)
		t.Fatalf("Upload failed: %v", err)
	}

	t.Logf("uploaded %d parts", len(parts))
	for _, p := range parts {
		t.Logf("part %d: ETag=%s", p.PartNumber, p.ETag)
	}

	err = store.CompleteMultipartUpload(ctx, uploadID, objectKey, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	t.Log("upload completed successfully")
}
