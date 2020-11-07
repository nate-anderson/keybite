package driver

import (
	"bufio"
	"fmt"
	"io"
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

const s3ErrNotFound = "NotFound"

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
		return BucketDriver{}, ErrInternalDriverFailure("authenticating s3", err)
	}

	client := s3.New(session)

	// validate existence and permissions of bucket
	_, err = client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		if isS3BucketNotExistErr(err) {
			return BucketDriver{}, ErrDataDirNotExist(bucketName, err)
		}
		return BucketDriver{}, ErrInternalDriverFailure("reading s3 bucket contents", err)
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
func (d BucketDriver) ReadPage(fileName string, indexName string, pageSize int) (map[uint64]string, []uint64, error) {
	d.setDownloaderIfNil()

	// download the remote file into a local temp file to read into memory
	// @TODO this can be improved by implementing a WriterAt and writing the
	// download's contents to a string instead of writing then reading a temp file
	tempFile, err := d.createTemporaryFile(fileName, indexName)
	if err != nil {
		return map[uint64]string{}, []uint64{}, err
	}
	defer tempFile.Close()

	err = d.downloadToFile(fileName, indexName, tempFile)
	if err != nil {
		return map[uint64]string{}, []uint64{}, err
	}

	vals := make(map[uint64]string, pageSize)
	orderedKeys := make([]uint64, 0, pageSize)

	scanner := bufio.NewScanner(tempFile)
	i := 0
	for scanner.Scan() {
		key, value, err := StringToKeyValue(scanner.Text())
		if err != nil {
			return vals, orderedKeys, ErrBadIndexData(indexName, fileName, err)
		}
		vals[key] = unescapeNewlines(value)
		orderedKeys = append(orderedKeys, key)
		i++
	}

	return vals, orderedKeys, nil

}

// ReadMapPage reads a remote file into a map page
func (d BucketDriver) ReadMapPage(fileName string, indexName string, pageSize int) (map[string]string, []string, error) {
	d.setDownloaderIfNil()

	// download the remote file into a local temp file to read into memory
	// @TODO this can be improved by implementing a WriterAt and writing the
	// download's contents to a string instead of writing then reading a temp file
	tempFile, err := d.createTemporaryFile(fileName, indexName)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	defer tempFile.Close()

	vals := make(map[string]string, pageSize)
	orderedKeys := make([]string, 0, pageSize)

	err = d.downloadToFile(fileName, indexName, tempFile)
	if err != nil {
		return vals, orderedKeys, err
	}

	scanner := bufio.NewScanner(tempFile)
	i := 0
	for scanner.Scan() {
		key, value, err := StringToMapKeyValue(scanner.Text())
		if err != nil {
			return vals, orderedKeys, ErrBadIndexData(indexName, fileName, err)
		}
		vals[key] = unescapeNewlines(value)
		orderedKeys = append(orderedKeys, key)
		i++
	}

	return vals, orderedKeys, nil
}

// WritePage persists a new or updated page as a file in the remote bucket
func (d BucketDriver) WritePage(vals map[uint64]string, orderedKeys []uint64, fileName string, indexName string) error {
	d.setUploaderIfNil()

	pageReader := newPageReader(vals, orderedKeys)
	cleanFileName := AddSuffixIfNotExist(fileName, d.pageExtension)
	filePath := path.Join(indexName, cleanFileName)

	// upload temporary file to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
		Body:   pageReader,
	})

	if err != nil {
		return ErrInternalDriverFailure("writing to s3 bucket", err)
	}

	return nil
}

// WriteMapPage persists a new or updated map page as a file in the remote bucket
func (d BucketDriver) WriteMapPage(vals map[string]string, orderedKeys []string, fileName string, indexName string) error {
	d.setUploaderIfNil()

	pageReader := newMapPageReader(vals, orderedKeys)
	cleanFileName := AddSuffixIfNotExist(fileName, d.pageExtension)
	filePath := path.Join(indexName, cleanFileName)

	// upload to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
		Body:   pageReader,
	})

	if err != nil {
		return ErrInternalDriverFailure("writing to s3 bucket", err)
	}

	return nil
}

// ListPages lists the page files in the bucket
func (d BucketDriver) ListPages(indexName string, desc bool) ([]string, error) {
	prefix := indexName + "/"
	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucketName),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		if isS3NotExistErr(err) {
			return []string{}, ErrIndexNotExist(indexName, err)
		}
		return []string{}, ErrInternalDriverFailure("reading contents of bucket folder", err)
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

	return sortFileNames(pages, d.pageExtension, desc), nil
}

// create a temporary file
func (d BucketDriver) createTemporaryFile(fileName string, indexName string) (*os.File, error) {
	currentMillis := timeToMillis(time.Now())
	tempName := fmt.Sprintf("%s-%s-%d%s.tmp", indexName, fileName, currentMillis, d.pageExtension)
	tempPath := path.Join("/tmp", tempName)
	file, err := os.Create(tempPath)
	if err != nil {
		return file, ErrInternalDriverFailure("creating temporary data file", err)
	}
	return file, nil

}

func (d BucketDriver) downloadToFile(fileName string, indexName string, dest *os.File) error {
	remotePath := path.Join(indexName, AddSuffixIfNotExist(fileName, d.pageExtension))
	_, err := d.s3Downloader.Download(dest,
		&s3.GetObjectInput{
			Bucket: aws.String(d.bucketName),
			Key:    aws.String(remotePath),
		},
	)

	if err != nil {
		if isS3NotExistErr(err) {
			if d.indexExists(indexName) {
				return ErrPageNotExist(indexName, fileName, err)
			}
			return ErrIndexNotExist(indexName, err)
		}
		log.Errorf("error fetching remote file %s", remotePath)
		return ErrInternalDriverFailure("downloading s3 file", err)
	}

	_, err = dest.Seek(0, io.SeekStart)
	if err != nil {
		return ErrInternalDriverFailure("resetting file cursor in tempfile", err)
	}

	return nil
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

	if err != nil {
		return ErrInternalDriverFailure("creating auto index", err)
	}

	return nil
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

	if err != nil {
		return ErrInternalDriverFailure("creating map index", err)
	}

	return nil
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
		return true, time.Now(), ErrInternalDriverFailure("listing index directory contents", err)
	}

	var maxLockTs time.Time

	for _, item := range resp.Contents {
		itemName := path.Base(*item.Key)
		if isLockfile(itemName) {
			ts, err := filenameToLockTimestamp(itemName)
			if err != nil {
				// file is not a valid lockfile
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

	lockExpires := time.Now().Add(d.lockDuration)
	expiresMillis := strconv.FormatInt(timeToMillis(lockExpires), 10)

	lockfileName := expiresMillis + d.pageExtension + lockfileExtension

	filePath := path.Join(indexName, lockfileName)

	// upload to S3
	_, err := d.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(filePath),
		Body:   strings.NewReader(""),
	})

	if err != nil {
		return ErrInternalDriverFailure("locking index", err)
	}
	return nil
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
		return ErrInternalDriverFailure("reading index contents", err)
	}

	var loopErr error
	for _, item := range resp.Contents {
		itemName := path.Base(*item.Key)
		if isLockfile(itemName) {
			_, err := d.s3Client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(d.bucketName),
				Key:    aws.String(*item.Key),
			})
			if err != nil {
				loopErr = err
				log.Errorf("deleting lockfile '%s' in index '%s' failed: %w", itemName, indexName, err)
				continue
			}
		}
	}

	if loopErr != nil {
		return ErrInternalDriverFailure("deleting lockfile in index", loopErr)
	}
	return nil
}

// DropAutoIndex permanently deletes all the data and directory for an auto index
func (d BucketDriver) DropAutoIndex(indexName string) error {
	// get list of object keys matching directory prefix
	prefix := indexName + "/"

	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return ErrInternalDriverFailure("reading s3 directory contents", err)
	}

	numItems := len(resp.Contents)
	deleteObjects := make([]*s3.ObjectIdentifier, numItems)

	for i, item := range resp.Contents {
		deleteObjects[i] = &s3.ObjectIdentifier{
			Key: item.Key,
		}
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(d.bucketName),
		Delete: &s3.Delete{
			Objects: deleteObjects,
		},
	}

	_, err = d.s3Client.DeleteObjects(deleteInput)
	if err != nil {
		if isS3NotExistErr(err) {
			return ErrIndexNotExist(indexName, err)
		}
		return ErrInternalDriverFailure("dropping index", err)
	}
	return nil
}

// DropMapIndex permanently deletes all the data and directory for a map index
func (d BucketDriver) DropMapIndex(indexName string) error {
	// get list of object keys matching directory prefix
	prefix := indexName + "/"

	resp, err := d.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return ErrInternalDriverFailure("reading s3 directory contents", err)
	}

	numItems := len(resp.Contents)
	deleteObjects := make([]*s3.ObjectIdentifier, numItems)

	for i, item := range resp.Contents {
		deleteObjects[i] = &s3.ObjectIdentifier{
			Key: item.Key,
		}
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(d.bucketName),
		Delete: &s3.Delete{
			Objects: deleteObjects,
		},
	}

	_, err = d.s3Client.DeleteObjects(deleteInput)
	if err != nil {
		if isS3NotExistErr(err) {
			return ErrIndexNotExist(indexName, err)
		}
		return ErrInternalDriverFailure("dropping index", err)
	}
	return nil
}

// DeletePage (for testing purposes)
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

	if err != nil {
		return ErrInternalDriverFailure("dropping index", err)
	}
	return nil
}

func (d BucketDriver) indexExists(indexName string) bool {
	_, err := d.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String("bucket_name"),
		Key:    aws.String("object_key"),
	})
	fmt.Println("err", err)
	if err != nil {
		return !isS3NotExistErr(err)
	}
	return true
}

// is the error a missing file or bucket error from S3?
func isS3NotExistErr(in error) bool {
	err, ok := in.(awserr.Error)
	if ok && (err.Code() == s3.ErrCodeNoSuchKey || err.Code() == s3ErrNotFound) {
		return true
	}

	return false
}

func isS3BucketNotExistErr(in error) bool {
	err, ok := in.(awserr.Error)
	if ok && err.Code() == s3.ErrCodeNoSuchBucket {
		return true
	}
	return false
}

// newPageReader constructs a page reader for an auto index page
func newPageReader(vals map[uint64]string, orderedKeys []uint64) io.Reader {
	var body string
	for _, key := range orderedKeys {
		line := fmt.Sprintf("%d:%s\n", key, escapeNewlines(vals[key]))
		body += line
	}

	return strings.NewReader(body)
}

// newMapPageReader constructs a page reader for a map page
func newMapPageReader(vals map[string]string, orderedKeys []string) io.Reader {
	var body string
	for _, key := range orderedKeys {
		line := fmt.Sprintf("%s:%s\n", key, escapeNewlines(vals[key]))
		body += line
	}

	return strings.NewReader(body)
}
