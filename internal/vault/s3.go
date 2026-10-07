package vault

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// LockMode is the S3 Object Lock retention mode.
//
//   - GOVERNANCE: nobody can delete a locked version unless they hold the special
//     s3:BypassGovernanceRetention permission. Good for demos and testing.
//   - COMPLIANCE: nobody can delete it before the date, not even the account root user.
//     This is the mode that actually defeats an attacker with admin credentials.
type LockMode string

const (
	Governance LockMode = "GOVERNANCE"
	Compliance LockMode = "COMPLIANCE"
)

// S3 is a Store on an S3 bucket created with Object Lock enabled. Every object it writes gets
// a retain-until date of now + Retention.
type S3 struct {
	client    *s3.Client
	bucket    string
	retention time.Duration
	mode      LockMode
}

// NewS3 creates the client. endpoint is for S3-compatible servers such as LocalStack.
func NewS3(ctx context.Context, bucket, endpoint string, retention time.Duration, mode LockMode) (*S3, error) {
	if mode == "" {
		mode = Governance
	}
	if mode != Governance && mode != Compliance {
		return nil, fmt.Errorf("vault: lock mode must be GOVERNANCE or COMPLIANCE, got %q", mode)
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // LocalStack serves http://host:4566/bucket/key
		}
	})
	return &S3{client: client, bucket: bucket, retention: retention, mode: mode}, nil
}

func (s *S3) String() string { return "s3://" + s.bucket }

// EnsureBucket creates the bucket with Object Lock enabled if it does not exist yet.
// Object Lock can only be switched on when a bucket is created (it also turns on versioning),
// so an existing bucket without it is reported as an error instead of being used unprotected.
func (s *S3) EnsureBucket(ctx context.Context) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(s.bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	var owned *types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) && !hasCode(err, "BucketAlreadyOwnedByYou") {
		return fmt.Errorf("vault: create bucket %s: %w", s.bucket, err)
	}
	lock, err := s.client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(s.bucket)})
	if err != nil || lock.ObjectLockConfiguration == nil ||
		lock.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		return fmt.Errorf("vault: bucket %s does not have Object Lock enabled", s.bucket)
	}
	return nil
}

func (s *S3) Put(ctx context.Context, key string, data []byte) (string, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:                    aws.String(s.bucket),
		Key:                       aws.String(key),
		Body:                      bytes.NewReader(data),
		ObjectLockMode:            types.ObjectLockMode(s.mode),
		ObjectLockRetainUntilDate: aws.Time(time.Now().Add(s.retention).UTC()),
		// S3 requires an integrity checksum on writes with Object Lock headers.
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return "", fmt.Errorf("vault: put %s: %w", key, err)
	}
	return aws.ToString(out.VersionId), nil
}

func (s *S3) Get(ctx context.Context, key, version string) ([]byte, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if version != "" {
		in.VersionId = aws.String(version)
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("vault: get %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3) Head(ctx context.Context, key string) (string, bool, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("vault: head %s: %w", key, err)
	}
	return aws.ToString(out.VersionId), true, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("vault: list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}

// DeleteVersion deletes one specific version permanently. Note it never sends the
// bypass-governance header: this code path must not be able to remove locked data.
func (s *S3) DeleteVersion(ctx context.Context, key, version string) error {
	if version == "" {
		// Without a version ID, S3 would only add a "delete marker" and keep the data,
		// which looks like a delete but is not one. Refuse to pretend.
		return errors.New("vault: S3 deletes need a version ID")
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), VersionId: aws.String(version),
	})
	if err != nil {
		if hasCode(err, "AccessDenied") || hasCode(err, "InvalidRequest") {
			return fmt.Errorf("%w: %s", ErrLocked, key)
		}
		return fmt.Errorf("vault: delete %s: %w", key, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	return errors.As(err, &nsk) || errors.As(err, &nf) || hasCode(err, "NoSuchVersion") || hasCode(err, "NotFound")
}

func hasCode(err error, code string) bool {
	var api smithy.APIError
	return errors.As(err, &api) && strings.EqualFold(api.ErrorCode(), code)
}
