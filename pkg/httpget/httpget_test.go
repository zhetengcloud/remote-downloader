package httpget

import (
	"context"
	"io"
	"testing"
	"time"
)

const testURL = "https://mirrors.aliyun.com/gradle/gradle-1.0-all.zip"

func TestDownloader_GetFileSize(t *testing.T) {
	d := NewDownloader()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	size, err := d.GetFileSize(ctx, testURL)
	if err != nil {
		t.Fatalf("GetFileSize failed: %v", err)
	}

	if size <= 0 {
		t.Fatalf("expected positive size, got %d", size)
	}

	t.Logf("file size: %d bytes (%.2f MB)", size, float64(size)/(1024*1024))
}

func TestDownloader_RangeDownload(t *testing.T) {
	d := NewDownloader()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	poolSize := 3
	chunkSize := int64(1024 * 1024) // 1MB chunks for testing

	infos, err := d.RangeDownload(ctx, testURL, poolSize, chunkSize)
	if err != nil {
		t.Fatalf("RangeDownload failed: %v", err)
	}

	var totalBytes int64
	var partCount int
	var errors []error

	for info := range infos {
		// Read and discard data
		n, err := io.Copy(io.Discard, info.Body())
		_ = info.Body().Close()

		if err != nil {
			errors = append(errors, err)
			t.Logf("part %d read error: %v", info.PartNumber(), err)
			continue
		}

		totalBytes += n
		partCount++

		t.Logf("part %d: size %d", info.PartNumber(), n)
	}

	if len(errors) > 0 {
		t.Fatalf("got %d errors", len(errors))
	}

	if partCount == 0 {
		t.Fatal("no parts downloaded")
	}

	t.Logf("downloaded %d parts, total %d bytes", partCount, totalBytes)
}

func TestDownloader_Download(t *testing.T) {
	d := NewDownloader()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Just read a small portion to verify it works
	data, err := d.Download(ctx, testURL)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	defer func() {
		_ = data.Close()
	}()

	// Read first 1KB
	buf := make([]byte, 1024)
	n, err := data.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	if n == 0 {
		t.Fatal("read 0 bytes")
	}

	t.Logf("read %d bytes", n)
}

func TestSplitRanges(t *testing.T) {
	tests := []struct {
		name      string
		totalSize int64
		chunkSize int64
		wantParts int
	}{
		{"exact fit", 100, 10, 10},
		{"with remainder", 105, 10, 11},
		{"single part", 5, 10, 1},
		{"large file", 100 * 1024 * 1024, 5 * 1024 * 1024, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := splitRanges(tt.totalSize, tt.chunkSize)
			if len(ranges) != tt.wantParts {
				t.Errorf("got %d parts, want %d", len(ranges), tt.wantParts)
			}

			// Verify ranges are contiguous
			var total int64
			for i, r := range ranges {
				total += r.Size()

				if i > 0 && r.Start != ranges[i-1].End+1 {
					t.Errorf("range %d not contiguous: previous end %d, current start %d",
						i, ranges[i-1].End, r.Start)
				}
			}

			if total != tt.totalSize {
				t.Errorf("total size %d, want %d", total, tt.totalSize)
			}
		})
	}
}

func TestRange_Size(t *testing.T) {
	tests := []struct {
		start int64
		end   int64
		want  int64
	}{
		{0, 9, 10},
		{100, 199, 100},
		{0, 0, 1},
		{10, 5, 0}, // invalid range
	}

	for _, tt := range tests {
		r := Range{Start: tt.start, End: tt.end}
		got := r.Size()
		if got != tt.want {
			t.Errorf("Range{%d, %d}.Size() = %d, want %d", tt.start, tt.end, got, tt.want)
		}
	}
}

func TestRange_HeaderValue(t *testing.T) {
	r := Range{Start: 0, End: 5242879}
	want := "bytes=0-5242879"
	got := r.HeaderValue()
	if got != want {
		t.Errorf("HeaderValue() = %q, want %q", got, want)
	}
}
