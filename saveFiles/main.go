package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
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
	Docs               []string `json:"docs"`
}

type MetadataSummary struct {
	GeneratedAt            string           `json:"generated_at"`
	MostRecentLastModified *string          `json:"most_recent_last_modified"`
	TotalAvgSizeLast5      float64          `json:"total_avg_size_last5"`
	TotalSizeBytes         int64            `json:"total_size_bytes"`
	Products               []ProductSummary `json:"products"`
	Docs                   []string         `json:"docs"`
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
		u.Concurrency = 3
		u.LeavePartsOnError = false // Clean up parts on failure
		u.ClientOptions = append(u.ClientOptions, func(o *s3.Options) {
			o.RetryMaxAttempts = 5
			o.RetryMode = aws.RetryModeAdaptive
		})
	})
	contentType := mimeTypeForFile(key)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        f,
		ContentType: aws.String(contentType),
		ACL:         types.ObjectCannedACLPublicRead,
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
	case strings.HasSuffix(lower, ".doc"):
		return "application/msword"
	case strings.HasSuffix(lower, ".docx"):
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case strings.HasSuffix(lower, ".pdf"):
		return "application/pdf"
	case strings.HasSuffix(lower, ".md"):
		return "text/markdown"
	case strings.HasSuffix(lower, ".csv"):
		return "text/csv"
	default:
		return "application/octet-stream"
	}
}

// processAndUpload handles downloading remote SFTP files to local OUTPUT_DIR and uploading to S3.
// It returns a slice of successfully saved local file paths (relative to outputDir or absolute local paths).
func processAndUpload(ctx context.Context, s3c *s3.Client, bucket string, sftpc *sftp.Client, outputDir string, remotes []string) []string {
	saved := make([]string, 0, len(remotes))
	for _, r := range remotes {
		remote := r
		if remote == "" || remote == "/" {
			continue
		}
		if !strings.HasPrefix(remote, "/") {
			remote = "/" + remote
		}
		localPath := filepath.Join(outputDir, filepath.FromSlash(strings.TrimPrefix(remote, "/")))
		log.Printf("Downloading %s -> %s", remote, localPath)
		err := retry(3, 2*time.Second, func() error {
			return copyFromSFTPToLocal(sftpc, remote, localPath)
		})
		if err != nil {
			log.Printf("ERROR downloading %s: %v", remote, err)
			continue
		}
		s3key := strings.TrimPrefix(filepath.ToSlash(remote), "/")
		log.Printf("Uploading to bucket=%s key=%s from %s", bucket, s3key, localPath)
		err = retry(3, 2*time.Second, func() error {
			return uploadFile(ctx, s3c, bucket, s3key, localPath)
		})
		if err != nil {
			log.Printf("ERROR uploading %s: %v", s3key, err)
			continue
		}
		log.Printf("Done %s", s3key)
		saved = append(saved, localPath)
	}
	return saved
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

func PandocConvert(inputPath, outputPath string, options []string) error {
	ext := filepath.Ext(inputPath)
	if (ext == ".doc" || ext == ".docx" || ext == ".txt") && filepath.Ext(outputPath) == ".pdf" {
		// Use LibreOffice for .doc/.docx to .pdf
		cmd := exec.Command("libreoffice", "--headless", "--convert-to", "pdf", inputPath, "--outdir", filepath.Dir(outputPath))
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("libreoffice conversion failed: %v, stderr: %s", err, stderr.String())
		}
		// LibreOffice outputs to outdir with same filename but .pdf extension
		generatedPath := filepath.Join(filepath.Dir(outputPath), filepath.Base(inputPath[:len(inputPath)-len(ext)])+".pdf")
		// Rename to desired outputPath if different
		if generatedPath != outputPath {
			if err := os.Rename(generatedPath, outputPath); err != nil {
				return fmt.Errorf("failed to rename %s to %s: %v", generatedPath, outputPath, err)
			}
		}
		return nil
	}

	if ext == ".doc" {
		cmd := exec.Command("libreoffice", "--headless", "--convert-to", "docx", inputPath, "--outdir", filepath.Dir(inputPath))
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("libreoffice conversion failed: %v", err)
		}
		inputPath = strings.ReplaceAll(inputPath, ".doc", ".docx")
	}

	args := append([]string{"-s", inputPath, "-o", outputPath}, options...)
	if filepath.Ext(outputPath) == ".pdf" {
		args = append(args, "--pdf-engine=pdflatex")
	} else if filepath.Ext(outputPath) == ".md" {
		args = append(args, []string{"--wrap=none", "--markdown-headings=atx", "--to=markdown"}...)
	}
	cmd := exec.Command("/usr/bin/pandoc", args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pandoc conversion failed: %v, stderr: %s", err, stderr.String())
	}

	return nil
}

func processDoc(ctx context.Context, s3c *s3.Client, s3Bucket, inputPath, outputDir string) error {
	ext := filepath.Ext(inputPath)
	if ext != ".doc" && ext != ".docx" && ext != ".txt" {
		// 	the only formats supported by pandoc
		return nil
	}
	outputPath := filepath.Join(filepath.Dir(inputPath), filepath.Base(inputPath[:len(inputPath)-len(filepath.Ext(inputPath))])+".pdf")
	options := []string{}
	if err := PandocConvert(inputPath, outputPath, options); err != nil {
		log.Fatalf("Conversion failed: %v", err)
	}
	log.Printf("Converted %s to %s", inputPath, outputPath)
	s3key := "custom" + strings.TrimPrefix(outputPath, outputDir)
	err := retry(3, 2*time.Second, func() error {
		return uploadFile(ctx, s3c, s3Bucket, s3key, outputPath)
	})
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
	s3Region := getenv("S3_REGION", "auto") // R2 uses "auto"
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

	// Upload metadata JSON to the root of the bucket before downloading files
	metaKey := filepath.Base(metadataPath)
	log.Printf("Access key %s", s3Access)
	log.Printf("Uploading metadata to bucket=%s key=%s from %s", s3Bucket, metaKey, metadataPath)
	err = retry(3, 2*time.Second, func() error {
		return uploadFile(ctx, s3c, s3Bucket, metaKey, metadataPath)
	})
	if err != nil {
		log.Fatalf("ERROR uploading metadata %s: %v", metaKey, err)
	}

	// Connect SFTP once
	sconn, err := connectSFTP(sftpHost, sftpPort, sftpUser, sftpKeyPath, sftpKeyPass)
	if err != nil {
		log.Fatalf("sftp connect error: %v", err)
	}
	defer sconn.Close()

	// First, process top-level docs from metadata summary
	savedTop := processAndUpload(ctx, s3c, s3Bucket, sconn.client, outputDir, summary.Docs)
	log.Printf("Saved top-level docs: %v", savedTop)
	for _, inputPath := range savedTop {
		processDoc(ctx, s3c, s3Bucket, inputPath, outputDir)
	}

	// Process products/files
	for _, p := range summary.Products {
		// First, process docs similarly to latest files
		savedDocs := processAndUpload(ctx, s3c, s3Bucket, sconn.client, outputDir, p.Docs)
		log.Printf("Saved product docs for %s: %v", p.Product, savedDocs)
		for _, inputPath := range savedDocs {
			processDoc(ctx, s3c, s3Bucket, inputPath, outputDir)
		}

		// Then process latest files if less than 100MB
		if p.AvgSizeLast5 != nil && *p.AvgSizeLast5 > 90000000 && *p.AvgSizeLast5 < 100000000 {
			savedLatest := processAndUpload(ctx, s3c, s3Bucket, sconn.client, outputDir, p.LatestFiles)
			log.Printf("Saved latest files for %s: %v", p.Product, savedLatest)
		}

	}
	log.Println("All done")
}
