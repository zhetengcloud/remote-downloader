# remote-downloader

Download files via HTTP byte-range requests and upload to cloud storage.

## Design

Focused architecture for downloading to cloud storage:

```
┌─────────────────────────────────────────────────────────────┐
│  httpget.RangeDownload(url, poolSize, chunkSize)            │
│                     ↓                                       │
│         <-chan storage.RangeInfo                            │
│                     ↓                                       │
│       oss.Upload(objectKey, uploadID, rangeCh)              │
└─────────────────────────────────────────────────────────────┘
```

## httpget

HTTP byte-range downloader. Returns `storage.RangeInfo` directly.

```go
import "go.zheteng.cloud/downloader/pkg/httpget"

d := httpget.NewDownloader()

// Get file size
size, _ := d.GetFileSize(ctx, url)

// Download with 5 workers, 5MB chunks - returns storage.RangeInfo directly!
rangeCh, _ := d.RangeDownload(ctx, url, 5, 5*1024*1024)

// Pass rangeCh directly to storage.Upload - no adapter needed
parts, _ := store.Upload(ctx, objectKey, uploadID, rangeCh, 5)
```

## storage

Storage interface and implementations.

### Interface

```go
package storage

type Storage interface {
    CreateMultipartUpload(ctx context.Context, objectKey string) (uploadID string, err error)
    CompleteMultipartUpload(ctx context.Context, uploadID string, objectKey string, parts []PartInfo) error
    AbortMultipartUpload(ctx context.Context, uploadID string, objectKey string) error
}

type RangeInfo interface {
    PartNumber() int
    Body() io.ReadCloser
}
```

### OSS Implementation

```go
import "go.zheteng.cloud/downloader/pkg/storage/oss"

cfg := oss.Config{
    Region:          "cn-hangzhou",
    AccessKeyID:     "key",
    AccessKeySecret: "secret",
    Bucket:          "bucket",
}

store, _ := oss.New(cfg, progressCh)

// Create multipart upload
uploadID, _ := store.CreateMultipartUpload(ctx, objectKey)

// Download - returns RangeInfo channel directly
rangeCh, _ := d.RangeDownload(ctx, url, 5, 5*1024*1024)

// Upload directly from channel - no adapter!
parts, _ := store.Upload(ctx, objectKey, uploadID, rangeCh, 5)
```

## Aliyun Function Compute

Deploy as FC function for serverless file downloading.

### Build

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o main ./cmd/multipart/
zip main.zip main
```

### Input Event

```json
{
  "url": "https://example.com/file.zip",
  "bucket": "my-bucket",
  "objectKey": "downloads/file.zip",
  "bucketRegion": "cn-hangzhou",
  "poolSize": 5,
  "chunkSize": 5242880
}
```

### Output

```json
{
  "success": true,
  "message": "upload completed",
  "objectKey": "downloads/file.zip",
  "parts": 10,
  "bytes": 52428800
}
```

### FC Configuration

- **Runtime**: Go 1.x
- **Handler**: main
- **Environment Variables**: Uses FC provided credentials
  - `ALIBABA_CLOUD_ACCESS_KEY_ID`
  - `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
  - `ALIBABA_CLOUD_SECURITY_TOKEN` (for STS)

## File Structure

```
.
├── cmd/
│   └── multipart/       # Aliyun Function Compute handler
│       └── main.go
├── pkg/
│   ├── httpget/         # HTTP download
│   │   ├── httpget.go
│   │   └── httpget_test.go
│   └── storage/         # Storage interface and implementations
│       ├── storage.go
│       └── oss/         # Aliyun OSS v2 implementation
│           ├── oss.go
│           ├── oss_test.go
│           └── .env.test
├── go.mod
└── README.md
```

## Design Principles

1. **Single purpose**: This project is for downloading to cloud storage
2. **Direct flow**: `httpget` returns `storage.RangeInfo` - no conversion needed
3. **Pipeline**: Channel connects download directly to upload
4. **Concurrent**: Both download and upload use worker pools
