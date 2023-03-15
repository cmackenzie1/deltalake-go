package storage

import (
	"context"
	"fmt"
	"io"
	unixpath "path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3EndpointResolver returns an endpoint resolver that always returns the given endpoint URL.
// This is used to override the default S3 endpoint resolver to use a custom endpoint like
// a local Minio instance, Cloudflare R2, Backblaze B2, etc.
func s3EndpointResolver(endpointURL string) aws.EndpointResolverWithOptionsFunc {
	return func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:           endpointURL,
			SigningRegion: region,
		}, nil
	}
}

type S3StorageOptions struct {
	// Region is the AWS region to use.
	Region string
	// Endpoint is the AWS endpoint to use.
	EndpointURL string

	// AccessKeyID is the AWS access key ID to use.
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key to use.
	SecretAccessKey string
	// SessionToken is the AWS session token to use.
	SessionToken string
}

type S3StorageOption func(*S3StorageOptions) error

func WithS3Region(region string) S3StorageOption {
	return func(o *S3StorageOptions) error {
		o.Region = region
		return nil
	}
}

func WithS3EndpointURL(endpointURL string) S3StorageOption {
	return func(o *S3StorageOptions) error {
		o.EndpointURL = endpointURL
		return nil
	}
}

func WithS3StaticCredentials(accessKeyID, secretAccessKey, sessionToken string) S3StorageOption {
	return func(o *S3StorageOptions) error {
		o.AccessKeyID = accessKeyID
		o.SecretAccessKey = secretAccessKey
		o.SessionToken = sessionToken
		return nil
	}
}

type S3Storage struct {
	bucket string
	prefix string

	cfg    aws.Config
	client *s3.Client
}

func NewS3Storage(bucket, prefix string, opts ...S3StorageOption) (*S3Storage, error) {
	o := &S3StorageOptions{}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}

	loadOpts := make([]func(*config.LoadOptions) error, 0)
	if o.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(o.Region))
	}
	if o.EndpointURL != "" {
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(s3EndpointResolver(o.EndpointURL)))
	}
	if o.AccessKeyID != "" && o.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(o.AccessKeyID, o.SecretAccessKey, o.SessionToken)))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, err
	}

	return &S3Storage{
		bucket: bucket,
		prefix: prefix,
		cfg:    cfg,
		client: s3.NewFromConfig(cfg),
	}, nil
}

func (s *S3Storage) fullpath(path string) string {
	return unixpath.Join(s.prefix, path) // only use unix-style paths with forward slashes
}

func (s *S3Storage) Put(path string, data io.Reader) error {
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullpath(path)),
		Body:   data,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Storage) Get(path string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullpath(path)),
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3Storage) Head(path string) (ObjectInfo, error) {
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullpath(path)),
	})
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Path:         path,
		Size:         head.ContentLength,
		LastModified: *head.LastModified,
	}, nil
}

func (s *S3Storage) Delete(path string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullpath(path)),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Storage) List(prefix string) ([]ObjectInfo, error) {
	ls := make([]ObjectInfo, 0)
	resp, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.fullpath(prefix)),
	})
	if err != nil {
		return nil, err
	}

	for _, obj := range resp.Contents {
		ls = append(ls, ObjectInfo{
			Path:         *obj.Key,
			Size:         obj.Size,
			LastModified: *obj.LastModified,
		})
	}
	return ls, nil
}

func (s *S3Storage) RootURI() string {
	return fmt.Sprintf("s3://%s/%s", s.bucket, s.prefix)
}
