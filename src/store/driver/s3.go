package driver

import (
	"bufio"
	"fmt"
	"keybite-http/util"
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// BucketDriver enables writing and reading indices from a remote S3 bucket
type BucketDriver struct {
	bucketName      string
	accessKeyID     string
	accessKeySecret string
	pageExtension   string
	s3Client        *s3.S3
	session         *session.Session
	s3Downloader    *s3manager.Downloader
	s3Uploader      *s3manager.Uploader
}

// NewBucketDriver instantiates a new bucket storage driver
func NewBucketDriver(
	pageExtension string,
	bucketName string,
	accessKeyID string,
	accessKeySecret string,
) (BucketDriver, error) {
	creds := credentials.NewStaticCredentials(accessKeyID, accessKeySecret, "")
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: creds,
	},
	)
	if err != nil {
		return BucketDriver{}, err
	}

	client := s3.New(session)

	// validate existence and permissions of bucket
	_, err = client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		return BucketDriver{}, err
	}

	return BucketDriver{
		bucketName:      bucketName,
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,
		pageExtension:   pageExtension,
		s3Client:        client,
		session:         session,
	}, nil
}

// ReadPage reads the contents of a page into a map
func (d BucketDriver) ReadPage(fileName string, indexName string, pageSize int) (map[int64]string, error) {
	d.setDownloaderIfNil()

	// download the remote file into a local temp file to read into memory
	// @TODO this can be improved by implementing a WriterAt and writing the
	// download's contents to a string instead of writing then reading a temp file
	tempFile := d.createTemporaryFile(fileName, indexName)
	defer tempFile.Close()
	err := d.downloadToFile(fileName, tempFile)
	if err != nil {
		return map[int64]string{}, err
	}

	vals := make(map[int64]string, pageSize)

	scanner := bufio.NewScanner(tempFile)
	for scanner.Scan() {
		key, value, err := util.StringToKeyValue(scanner.Text())
		if err != nil {
			return vals, err
		}
		vals[key] = value
	}

	return vals, nil

}

// ReadMapPage reads a remote file into a map page
func (d BucketDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[uint64]string, error) {
	d.setDownloaderIfNil()

	// download the remote file into a local temp file to read into memory
	// @TODO this can be improved by implementing a WriterAt and writing the
	// download's contents to a string instead of writing then reading a temp file
	tempFile := d.createTemporaryFile(fileName, indexName)
	defer tempFile.Close()
	err := d.downloadToFile(fileName, tempFile)
	if err != nil {
		return map[uint64]string{}, err
	}

	vals := make(map[uint64]string, pageSize)
	scanner := bufio.NewScanner(tempFile)
	for scanner.Scan() {
		key, value, err := util.StringToMapKeyValue(scanner.Text())
		if err != nil {
			return vals, err
		}
		vals[key] = value
	}

	return vals, nil
}

// WritePage persists a new or updated page as a file in the remote bucket
func (d BucketDriver) WritePage(vals map[int64]string, fileName string, indexName string) error {
	d.setUploaderIfNil()

	// create and write data to temporary file
	tempFile := d.createTemporaryFile(fileName, indexName)
	defer tempFile.Close()

	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		_, err := tempFile.Write([]byte(line))
		if err != nil {
			return err
		}
	}

	// upload temporary file to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(fileName),
		Body:   tempFile,
	})

	return err
}

// WriteMapPage persists a new or updated map page as a file in the remote bucket
func (d BucketDriver) WriteMapPage(vals map[uint64]string, fileName string, indexName string) error {
	d.setUploaderIfNil()

	// create and write data to temporary file
	tempFile := d.createTemporaryFile(fileName, indexName)
	defer tempFile.Close()

	for key, value := range vals {
		line := fmt.Sprintf("%d:%s\n", key, value)
		_, err := tempFile.Write([]byte(line))
		if err != nil {
			return err
		}
	}

	// upload temporary file to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(fileName),
		Body:   tempFile,
	})

	return err
}

// ListPages lists the page files in the bucket
func (d BucketDriver) ListPages(indexName string) ([]string, error) {
	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(d.bucketName)})
	if err != nil {
		return []string{}, err
	}

	pages := make([]string, len(resp.Contents))
	for i, item := range resp.Contents {
		pages[i] = *item.Key
	}

	return pages, nil
}

// create a temporary file
func (d BucketDriver) createTemporaryFile(fileName string, indexName string) *os.File {
	currentMillis := util.MakeTimestamp()
	tempName := fmt.Sprintf("%s-%s-%d.%s.tmp", indexName, fileName, currentMillis, d.pageExtension)
	tempPath := path.Join("/tmp", tempName)
	file, err := os.Create(tempPath)
	if err != nil {
		log.Printf("error creating temporary file: %v\n", err)
		panic(err)
	}
	return file
}

func (d BucketDriver) downloadToFile(fileName string, dest *os.File) error {
	_, err := d.s3Downloader.Download(dest,
		&s3.GetObjectInput{
			Bucket: aws.String(d.bucketName),
			Key:    aws.String(fileName),
		},
	)
	return err
}

func (d *BucketDriver) setDownloaderIfNil() {
	if d.s3Downloader != nil {
		return
	}

	downloader := s3manager.NewDownloader(d.session)
	d.s3Downloader = downloader
}

func (d *BucketDriver) setUploaderIfNil() {
	if d.s3Uploader != nil {
		return
	}

	uploader := s3manager.NewUploader(d.session)
	d.s3Uploader = uploader
}

// CreateAutoIndex creates the folder for an auto index in the data dir
func (d BucketDriver) CreateAutoIndex(indexName string) error {
	// trailing slash in key represents a "folder" in s3
	// https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
	indexKey := indexName + "/"
	_, err := d.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(indexKey),
		Body:   nil,
	})

	return err
}

// CreateMapIndex creates the folder for a map index in the data dir
func (d BucketDriver) CreateMapIndex(indexName string) error {
	// trailing slash in key represents a "folder" in s3
	// https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
	indexKey := indexName + "/"
	_, err := d.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(indexKey),
		Body:   nil,
	})

	return err
}
