package driver

import (
	"bufio"
	"fmt"
	"io"
	"keybite/util"
	"keybite/util/log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	lockDuration    time.Duration
}

// NewBucketDriver instantiates a new bucket storage driver
func NewBucketDriver(
	pageExtension string,
	bucketName string,
	accessKeyID string,
	accessKeySecret string,
	accessKeyToken string,
	lockDuration time.Duration,
) (BucketDriver, error) {
	creds := credentials.NewStaticCredentials(accessKeyID, accessKeySecret, accessKeyToken)
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
		lockDuration:    lockDuration,
	}, nil
}

// ReadPage reads the contents of a page into a map
func (d BucketDriver) ReadPage(fileName string, indexName string, pageSize int) (map[int64]string, error) {
	d.setDownloaderIfNil()

	// download the remote file into a local temp file to read into memory
	// @TODO this can be improved by implementing a WriterAt and writing the
	// download's contents to a string instead of writing then reading a temp file
	remotePath := path.Join(indexName, util.AddSuffixIfNotExist(fileName, d.pageExtension))
	tempFile, err := d.createTemporaryFile(fileName, indexName)
	if err != nil {
		return map[int64]string{}, err
	}
	defer tempFile.Close()

	err = d.downloadToFile(remotePath, tempFile)
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
	tempFile, err := d.createTemporaryFile(fileName, indexName)
	if err != nil {
		return map[uint64]string{}, err
	}

	defer tempFile.Close()

	remotePath := path.Join(indexName, util.AddSuffixIfNotExist(fileName, d.pageExtension))

	vals := make(map[uint64]string, pageSize)

	err = d.downloadToFile(remotePath, tempFile)
	if err != nil {
		return vals, err
	}

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

	pageReader := NewPageReader(vals)
	cleanFileName := util.AddSuffixIfNotExist(fileName, d.pageExtension)
	filePath := path.Join(indexName, cleanFileName)

	// upload temporary file to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
		Body:   pageReader,
	})

	return err
}

// WriteMapPage persists a new or updated map page as a file in the remote bucket
func (d BucketDriver) WriteMapPage(vals map[uint64]string, fileName string, indexName string) error {
	d.setUploaderIfNil()

	pageReader := NewMapPageReader(vals)
	cleanFileName := util.AddSuffixIfNotExist(fileName, d.pageExtension)
	filePath := path.Join(indexName, cleanFileName)

	// upload to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
		Body:   pageReader,
	})

	return err
}

// ListPages lists the page files in the bucket
func (d BucketDriver) ListPages(indexName string) ([]string, error) {
	prefix := indexName + "/"
	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucketName),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		return []string{}, err
	}

	pages := []string{}
	for _, item := range resp.Contents {
		itemName := path.Base(*item.Key)
		// the folder marker is just an empty file, don't include it in results
		if itemName == indexName {
			continue
		}
		// strip prefixes
		pages = append(pages, itemName)
	}

	return pages, nil
}

// create a temporary file
func (d BucketDriver) createTemporaryFile(fileName string, indexName string) (*os.File, error) {
	currentMillis := util.MakeTimestamp()
	tempName := fmt.Sprintf("%s-%s-%d%s.tmp", indexName, fileName, currentMillis, d.pageExtension)
	tempPath := path.Join("/tmp", tempName)
	return os.Create(tempPath)

}

func (d BucketDriver) downloadToFile(remotePath string, dest *os.File) error {
	_, err := d.s3Downloader.Download(dest,
		&s3.GetObjectInput{
			Bucket: aws.String(d.bucketName),
			Key:    aws.String(remotePath),
		},
	)

	if err != nil {
		if isS3NotExistErr(err) {
			return ErrNotExist(remotePath)
		}
		log.Errorf("error fetching remote file %s", remotePath)
		return err
	}

	_, err = dest.Seek(0, io.SeekStart)

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

// DeletePage deletes a map or index page from the S3 bucket
func (d BucketDriver) DeletePage(indexName string, fileName string) error {
	filePath := path.Join(indexName, fileName)
	_, err := d.s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
	})
	if err != nil {
		return err
	}

	err = d.s3Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
	})
	return err
}

// DeleteIndex deletes an index from the bucket
func (d BucketDriver) DeleteIndex(indexName string) error {
	_, err := d.s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(indexName),
	})
	if err != nil {
		return err
	}

	err = d.s3Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(indexName),
	})
	return err
}

// IndexIsLocked checks if the specified index is locked and returns the timestamp it expires at
func (d BucketDriver) IndexIsLocked(indexName string) (bool, time.Time, error) {
	log.Debugf("checking index %s for write locks", indexName)
	prefix := indexName + "/"
	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucketName),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		return true, time.Now(), err
	}

	var maxLockTs time.Time

	for _, item := range resp.Contents {
		itemName := path.Base(*item.Key)
		if isLockfile(itemName) {
			log.Debugf("found lockfile %s in index %s", itemName, indexName)
			ts, err := filenameToLockTimestamp(itemName)
			if err != nil {
				continue
			}
			if ts.After(maxLockTs) {
				maxLockTs = ts
			}
		}
	}

	expire := maxLockTs.Add(d.lockDuration)
	isLocked := maxLockTs.After(time.Now())

	return isLocked, expire, nil
}

// LockIndex marks an index as locked for writing
func (d BucketDriver) LockIndex(indexName string) error {
	log.Debugf("locking index %s for writes", indexName)
	d.setUploaderIfNil()

	currentMillis := strconv.FormatInt(util.MakeTimestamp(), 10)
	lockfileName := currentMillis + d.pageExtension + lockfileExtension

	filePath := path.Join(indexName, lockfileName)

	// upload to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
		Body:   strings.NewReader(""),
	})

	log.Debugf("created lockfile %s", filePath)

	return err
}

// UnlockIndex deletes all write lockfiles in an index
func (d BucketDriver) UnlockIndex(indexName string) error {
	log.Debugf("unlocking index %s for writes", indexName)
	prefix := indexName + "/"

	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return err
	}

	for _, item := range resp.Contents {
		itemName := path.Base(*item.Key)
		if isLockfile(itemName) {
			log.Debugf("deleting lockfile %s", itemName)
			_, err := d.s3Client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(d.bucketName),
				Key:    aws.String(*item.Key),
			})
			if err != nil {
				log.Errorf("Error deleting index write lockfile! %s", err.Error())
				continue
			}
		}
	}

	return nil
}

// is the error a missing file or bucket error from S3?
func isS3NotExistErr(in error) bool {
	err, ok := in.(awserr.Error)
	if !ok {
		return false
	}

	if err.Code() == s3.ErrCodeNoSuchKey || err.Code() == s3.ErrCodeNoSuchBucket {
		return true
	}

	return false
}
