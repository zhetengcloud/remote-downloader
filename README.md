# remote-downloader

Download files via HTTP byte-range requests.

## Design

Simple, focused HTTP downloader with byte-range support:

```
┌─────────────────────────────────────────┐
│        httpget.Downloader              │
│                                        │
│  RangeDownload(url, poolSize, chunkSize)
│       ↓                                │
│  [HEAD] → get file size                │
│  [Split] → ranges                      │
│  [Workers] → concurrent GET ranges     │
│       ↓                                │
│  <-chan Result {Range, PartNum, Data}  │
└─────────────────────────────────────────┘
```

## Usage

```go
d := httpget.NewDownloader()

// Download with 5 concurrent workers, 5MB chunks
results, err := d.RangeDownload(ctx, "https://example.com/file.zip", 5, 5*1024*1024)
if err != nil {
    log.Fatal(err)
}

for result := range results {
    if result.Error != nil {
        log.Printf("part %d failed: %v\n", result.PartNum, result.Error)
        continue
    }
    
    // Use result.Data (must close)
    data, err := io.ReadAll(result.Data)
    _ = result.Data.Close()
    
    log.Printf("part %d: range [%d-%d], size %d\n",
        result.PartNum, result.Range.Start, result.Range.End, len(data))
}
```

## API

### Downloader

```go
type Downloader struct{}

func NewDownloader() *Downloader
func (d *Downloader) GetFileSize(ctx context.Context, url string) (int64, error)
func (d *Downloader) RangeDownload(ctx context.Context, url string, poolSize int, chunkSize int64) (<-chan Result, error)
func (d *Downloader) Download(ctx context.Context, url string) (io.ReadCloser, error)
```

### Types

```go
type Range struct {
    Start int64
    End   int64
}

type Result struct {
    Range   Range
    PartNum int           // 1-based part number
    Data    io.ReadCloser // must be closed by consumer
    Error   error
}
```

## Storage (separate package)

Storage interface is separate and independent:

```go
package storage

type Storage interface {
    CreateMultipartUpload(ctx context.Context, objectKey string) (uploadID string, err error)
    UploadPart(ctx context.Context, uploadID string, partNumber int, content io.Reader) (etag string, err error)
    CompleteMultipartUpload(ctx context.Context, uploadID string, parts []PartInfo) error
    AbortMultipartUpload(ctx context.Context, uploadID string) error
}
```

The caller is responsible for coordinating download results with storage uploads.

## File Structure

```
.
├── pkg/
│   ├── httpget/         # HTTP download only
│   │   └── httpget.go
│   └── storage/         # Storage interface
│       └── storage.go
├── go.mod
└── README.md
```

## Design Principles

- **Single Responsibility**: `httpget` only downloads, `storage` only uploads
- **No coupling**: Download doesn't know about storage
- **Caller controls**: Caller decides what to do with downloaded data
- **Simple API**: One method for range download, returns channel of results
