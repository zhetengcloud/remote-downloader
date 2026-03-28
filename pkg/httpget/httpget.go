// Package httpget
package httpget

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"go.zheteng.cloud/downloader/pkg/storage"
)

// Range represents a byte range for HTTP Range request.
type Range struct {
	Start int64
	End   int64
}

// Size returns the size of this range.
func (r Range) Size() int64 {
	if r.End < r.Start {
		return 0
	}
	return r.End - r.Start + 1
}

// HeaderValue returns the Range header value (e.g., "bytes=0-5242879").
func (r Range) HeaderValue() string {
	return fmt.Sprintf("bytes=%d-%d", r.Start, r.End)
}

// Downloader downloads files via HTTP byte-range requests.
type Downloader struct {
	client *http.Client
}

// NewDownloader creates a new Downloader.
func NewDownloader() *Downloader {
	return &Downloader{
		client: &http.Client{},
	}
}

// GetFileSize performs a HEAD request to get Content-Length.
func (d *Downloader) GetFileSize(ctx context.Context, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HEAD request failed: %s", resp.Status)
	}

	return resp.ContentLength, nil
}

// RangeDownload downloads a file using byte-range requests concurrently.
// It splits the file into ranges based on chunkSize and downloads them with a worker pool.
// Returns a channel of RangeInfo that the caller must consume.
// Each RangeInfo's Body must be closed by the consumer.
// The channel is closed when all ranges are processed.
func (d *Downloader) RangeDownload(ctx context.Context, url string, poolSize int, chunkSize int64) (<-chan storage.RangeInfo, error) {
	if poolSize <= 0 {
		poolSize = 3
	}
	if chunkSize <= 0 {
		chunkSize = 5 * 1024 * 1024 // 5MB default
	}

	totalSize, err := d.GetFileSize(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("get file size: %w", err)
	}

	ranges := splitRanges(totalSize, chunkSize)
	resultCh := make(chan storage.RangeInfo, poolSize)

	go func() {
		defer close(resultCh)

		rangeCh := make(chan struct {
			r       Range
			partNum int
		}, poolSize)

		// Start workers
		var wg sync.WaitGroup
		wg.Add(poolSize)
		for range poolSize {
			go func() {
				defer wg.Done()
				for item := range rangeCh {
					select {
					case <-ctx.Done():
						return
					default:
					}

					data, err := d.downloadRange(ctx, url, item.r)
					if err != nil {
						// Send error as a special RangeInfo that carries the error
						resultCh <- &errorRangeInfo{partNum: item.partNum, err: err}
						continue
					}

					resultCh <- &rangeInfo{
						r:       item.r,
						partNum: item.partNum,
						data:    data,
					}
				}
			}()
		}

		// Send ranges
		go func() {
			for i, r := range ranges {
				select {
				case rangeCh <- struct {
					r       Range
					partNum int
				}{r: r, partNum: i + 1}:
				case <-ctx.Done():
					close(rangeCh)
					return
				}
			}
			close(rangeCh)
		}()

		wg.Wait()
	}()

	return resultCh, nil
}

// rangeInfo implements storage.RangeInfo
type rangeInfo struct {
	r       Range
	partNum int
	data    io.ReadCloser
}

func (ri *rangeInfo) PartNumber() int     { return ri.partNum }
func (ri *rangeInfo) Body() io.ReadCloser { return ri.data }

// errorRangeInfo carries an error through the channel
type errorRangeInfo struct {
	partNum int
	err     error
}

func (eri *errorRangeInfo) PartNumber() int     { return eri.partNum }
func (eri *errorRangeInfo) Body() io.ReadCloser { return &errorReadCloser{err: eri.err} }

type errorReadCloser struct {
	err error
}

func (erc *errorReadCloser) Read(p []byte) (int, error) { return 0, erc.err }
func (erc *errorReadCloser) Close() error               { return nil }

// Download downloads the entire file (no range request).
// Returns a ReadCloser that must be closed by the caller.
func (d *Downloader) Download(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET request failed: %s", resp.Status)
	}

	return resp.Body, nil
}

// downloadRange downloads a specific byte range.
func (d *Downloader) downloadRange(ctx context.Context, url string, r Range) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Range", r.HeaderValue())

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	return resp.Body, nil
}

// SetHTTPClient allows customizing the HTTP client.
func (d *Downloader) SetHTTPClient(client *http.Client) {
	d.client = client
}

// splitRanges splits the file into byte ranges based on chunkSize.
func splitRanges(totalSize, chunkSize int64) []Range {
	if totalSize <= 0 || chunkSize <= 0 {
		return nil
	}

	numRanges := (totalSize + chunkSize - 1) / chunkSize
	ranges := make([]Range, 0, numRanges)

	for i := range numRanges {
		start := i * chunkSize
		end := start + chunkSize - 1
		if end >= totalSize {
			end = totalSize - 1
		}
		ranges = append(ranges, Range{
			Start: start,
			End:   end,
		})
	}

	return ranges
}
