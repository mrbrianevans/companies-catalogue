package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Types mirrored from metadataSummary/types.ts

type ProductSummary struct {
	Product            string   `json:"product"`
	LatestFiles        []string `json:"latest_files"`
	LatestDate         string   `json:"latest_date"`
	LatestLastModified string   `json:"latest_last_modified"`
	AvgIntervalDays    *float64 `json:"avg_interval_days"`
	AvgSizeLast5       *float64 `json:"avg_size_last5"`
	Last5Dates         []string `json:"last5_dates"`
}

type MetadataSummary struct {
	GeneratedAt            string           `json:"generated_at"`
	MostRecentLastModified *string          `json:"most_recent_last_modified"`
	TotalAvgSizeLast5      float64          `json:"total_avg_size_last5"`
	TotalSizeBytes         int64            `json:"total_size_bytes"`
	Products               []ProductSummary `json:"products"`
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}

type sftpConn struct {
	client *sftp.Client
}

func (c *sftpConn) Close() {
	if c.client != nil {
		_ = c.client.Close()
	}
}

func connectSFTP(host string, port int, username, keyPath, passphrase string) (*sftpConn, error) {
	pkData, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	var signer ssh.Signer
	if passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(pkData, []byte(passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(pkData)
	}
	if err != nil {
		return nil, fmt.Errorf("parse key: %w", err)
	}
	cfg := &ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Consider pinning host key in future
		Timeout:         30 * time.Second,
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial: %w", err)
	}
	client, err := sftp.NewClient(conn, sftp.MaxPacket(1<<15))
	if err != nil {
		return nil, fmt.Errorf("sftp new client: %w", err)
	}
	return &sftpConn{client: client}, nil
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

func copyFromSFTPToLocal(sftpc *sftp.Client, remotePath, localPath string) error {
	// Ensure local dir
	dir := filepath.Dir(localPath)
	if err := ensureDir(dir); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	// Open remote
	rf, err := sftpc.Open(remotePath)
	if err != nil {
		return fmt.Errorf("open remote %s: %w", remotePath, err)
	}
	defer rf.Close()
	// Create local temp then rename (atomic-ish)
	tmp := localPath + ".part"
	lf, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create local %s: %w", tmp, err)
	}
	defer func() { lf.Close(); os.Remove(tmp) }()
	bufw := bufio.NewWriterSize(lf, 2*1024*1024)
	if _, err := io.Copy(bufw, rf); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	if err := bufw.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if err := lf.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, localPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

func buildS3Client(ctx context.Context, endpoint, region, accessKey, secretKey string) (*s3.Client, error) {
	var cfg aws.Config
	var err error
	// If custom endpoint provided (e.g., R2), construct config manually
	if endpoint != "" {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		)
		if err != nil {
			return nil, err
		}
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               endpoint,
				HostnameImmutable: true,
			}, nil
		})
		cfg.EndpointResolverWithOptions = resolver
	} else {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		)
		if err != nil {
			return nil, err
		}
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// R2 prefers path-style; keep it enabled to be generic for custom endpoints
		o.UsePathStyle = true
	})
	return client, nil
}

func uploadFile(ctx context.Context, s3c *s3.Client, bucket, key, filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()
	uploader := manager.NewUploader(s3c, func(u *manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024 // 64MB parts for large files
	})
	contentType := mimeTypeForFile(key)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        f,
		ContentType: aws.String(contentType),
		ACL:         types.ObjectCannedACLPrivate,
	})
	return err
}

func mimeTypeForFile(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".json"):
		return "application/json"
	case strings.HasSuffix(lower, ".txt") || strings.HasSuffix(lower, ".dat"):
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		var nerr net.Error
		if errors.As(err, &nerr) && !nerr.Temporary() {
			// non-temporary, don't retry
			break
		}
		time.Sleep(sleep)
		sleep *= 2
	}
	return err
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	// Env
	metadataPath := getenv("METADATA_PATH", "/output/sftp_file_metadata_summary.json")
	outputDir := getenv("OUTPUT_DIR", "/output")

	sftpHost := getenv("SFTP_HOST", "bulk-live.companieshouse.gov.uk")
	sftpPortStr := getenv("SFTP_PORT", "22")
	sftpPort := 22
	fmt.Sscanf(sftpPortStr, "%d", &sftpPort)
	sftpUser := mustGetenv("SFTP_USERNAME")
	sftpKeyPath := getenv("SFTP_KEY_PATH", "/root/.ssh/ch_key")
	sftpKeyPass := getenv("SFTP_KEY_PASSPHRASE", "")

	s3Endpoint := getenv("S3_ENDPOINT", "") // e.g., https://<accountid>.r2.cloudflarestorage.com
	s3Region := getenv("S3_REGION", "auto")   // R2 uses "auto"
	s3Access := mustGetenv("S3_ACCESS_KEY_ID")
	s3Secret := mustGetenv("S3_SECRET_ACCESS_KEY")
	s3Bucket := mustGetenv("S3_BUCKET")

	// Read metadata
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		log.Fatalf("failed reading metadata: %v", err)
	}
	var summary MetadataSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		log.Fatalf("failed parsing metadata: %v", err)
	}
	if len(summary.Products) == 0 {
		log.Println("No products found in summary; exiting")
		return
	}

	// Build S3 client once
	ctx := context.Background()
	s3c, err := buildS3Client(ctx, s3Endpoint, s3Region, s3Access, s3Secret)
	if err != nil {
		log.Fatalf("s3 client error: %v", err)
	}

	// Connect SFTP once
	sconn, err := connectSFTP(sftpHost, sftpPort, sftpUser, sftpKeyPath, sftpKeyPass)
	if err != nil {
		log.Fatalf("sftp connect error: %v", err)
	}
	defer sconn.Close()

	// Process products/files
	for _, p := range summary.Products {
		for _, remote := range p.LatestFiles {
			if remote == "" || remote == "/" {
				continue
			}
			// Ensure remote path starts with '/'
			if !strings.HasPrefix(remote, "/") {
				remote = "/" + remote
			}
			// Local path: outputDir + remote path (preserve structure)
			localPath := filepath.Join(outputDir, filepath.FromSlash(strings.TrimPrefix(remote, "/")))
			log.Printf("Downloading %s -> %s", remote, localPath)
			err := retry(3, 2*time.Second, func() error {
				return copyFromSFTPToLocal(sconn.client, remote, localPath)
			})
			if err != nil {
				log.Printf("ERROR downloading %s: %v", remote, err)
				continue
			}

			// Upload to S3 with key equal to remote path without leading '/'
			s3key := strings.TrimPrefix(filepath.ToSlash(remote), "/")
			log.Printf("Uploading to bucket=%s key=%s from %s", s3Bucket, s3key, localPath)
			err = retry(3, 2*time.Second, func() error {
				return uploadFile(ctx, s3c, s3Bucket, s3key, localPath)
			})
			if err != nil {
				log.Printf("ERROR uploading %s: %v", s3key, err)
				continue
			}
			log.Printf("Done %s", s3key)
		}
	}
	log.Println("All done")
}
