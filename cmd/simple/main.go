package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/fc-runtime-go-sdk/fc"
)

// Event represents the function input event.
type Event struct {
	URL          string `json:"url"`
	Bucket       string `json:"bucket"`
	ObjectKey    string `json:"objectKey"`
	BucketRegion string `json:"bucketRegion"`
}

// Result represents the function output.
type Result struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ObjectKey string `json:"objectKey"`
	Bytes     int64  `json:"bytes"`
}

// HandleRequest handles the FC event.
func HandleRequest(ctx context.Context, event Event) (Result, error) {
	// Create HTTP client and request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, event.URL, nil)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("create request: %v", err)}, nil
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("download file: %v", err)}, nil
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return Result{Success: false, Message: fmt.Sprintf("bad status: %s", resp.Status)}, nil
	}

	// Get content length if available
	var contentLength int64
	if resp.ContentLength > 0 {
		contentLength = resp.ContentLength
	}

	// Get credentials from environment
	accessKeyID := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	securityToken := os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN")

	// Create OSS client
	creds := credentials.NewStaticCredentialsProvider(accessKeyID, accessKeySecret, securityToken)
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(creds).
		WithRegion(event.BucketRegion)

	client := oss.NewClient(cfg)

	// Pipe the HTTP response body to OSS
	putReq := &oss.PutObjectRequest{
		Bucket: oss.Ptr(event.Bucket),
		Key:    oss.Ptr(event.ObjectKey),
		Body:   resp.Body,
	}

	_, err = client.PutObject(ctx, putReq)
	if err != nil {
		return Result{Success: false, Message: fmt.Sprintf("upload to oss: %v", err)}, nil
	}

	return Result{
		Success:   true,
		Message:   "upload completed",
		ObjectKey: event.ObjectKey,
		Bytes:     contentLength,
	}, nil
}

func main() {
	fc.Start(HandleRequest)
}
