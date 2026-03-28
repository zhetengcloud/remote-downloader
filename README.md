# remote-downloader

Download files via HTTP byte-range requests and upload to cloud storage.

## Design

Decoupled architecture: `httpget` downloads, `storage` uploads, caller coordinates.

```
┌─────────────────────────────────────────────────────────────┐
│                        Caller                                │
│  ┌─────────────┐         ┌─────────────┐                   │
│  │  httpget    │  chan   │   storage   │                   │
│  │  Download   │ ───────▶│   Upload    │                   │
│  └─────────────┘         └─────────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

## httpget

HTTP byte-range downloader.

```go
import "go.zheteng.cloud/downloader/pkg/httpget"

d := httpget.NewDownloader()

// Get file size
size, _ := d.GetFileSize(ctx, url)

// Download with 5 workers, 5MB chunks
results, _ := d.RangeDownload(ctx, url, 5, 5*1024*1024)

for result := range results {
    if result.Error != nil {
        continue
    }
    // Use result.PartNum, result.Data
}
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

// Upload from channel with 5 workers
parts, _ := store.Upload(ctx, objectKey, uploadID, rangeCh, 5)

// Complete
store.CompleteMultipartUpload(ctx, uploadID, objectKey, parts)
```

## Usage Example

```go
package main

import (
    "context"
    "io"
    "go.zheteng.cloud/downloader/pkg/httpget"
    "go.zheteng.cloud/downloader/pkg/storage"
    "go.zheteng.cloud/downloader/pkg/storage/oss"
)

// Adapter: httpget.Result -> storage.RangeInfo
type resultAdapter struct {
    r httpget.Result
}

func (a resultAdapter) PartNumber() int       { return a.r.PartNum }
func (a resultAdapter) Body() io.ReadCloser   { return a.r.Data }

func main() {
    ctx := context.Background()
    
    // 1. Create downloader
    d := httpget.NewDownloader()
    
    // 2. Create OSS storage
    store, _ := oss.New(oss.Config{
        Region: "cn-hangzhou",
        // ... credentials
    }, nil)
    
    // 3. Create multipart upload
    uploadID, _ := store.CreateMultipartUpload(ctx, "file.zip")
    
    // 4. Download and bridge to upload channel
    results, _ := d.RangeDownload(ctx, "https://example.com/file.zip", 5, 5*1024*1024)
    
    rangeCh := make(chan storage.RangeInfo, 10)
    go func() {
        defer close(rangeCh)
        for r := range results {
            if r.Error != nil {
                continue
            }
            rangeCh <- resultAdapter{r}
        }
    }()
    
    // 5. Upload
    parts, err := store.Upload(ctx, "file.zip", uploadID, rangeCh, 5)
    if err != nil {
        store.AbortMultipartUpload(ctx, uploadID, "file.zip")
        return
    }
    
    // 6. Complete
    store.CompleteMultipartUpload(ctx, uploadID, "file.zip", parts)
}
```

## File Structure

```
.
├── pkg/
│   ├── httpget/         # HTTP download only
│   │   ├── httpget.go
│   │   └── httpget_test.go
│   └── storage/         # Storage interface and implementations
│       ├── storage.go
│       └── oss/         # Aliyun OSS v2 implementation
│           ├── oss.go
│           └── oss_test.go
├── go.mod
└── README.md
```

## Design Principles

1. **No coupling**: `httpget` doesn't know about `storage`, `storage` doesn't know about `httpget`
2. **Interface-based**: `storage.RangeInfo` is the bridge between them
3. **Caller controls**: Caller creates channel, adapts types, coordinates flow
4. **Concurrent uploads**: `OSS.Upload()` accepts poolSize for concurrent part uploads
