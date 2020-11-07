package driver

import (
	"fmt"
	"keybite/config"
	"keybite/util"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const pageExtension string = ".kb"

var conf config.Config

func init() {
	var err error
	conf, err = config.MakeConfig("test.env")
	if err != nil {
		panic(err)
	}
}

func getEnvCreds() (accessKeyID string, accessKeySecret string, bucketName string, err error) {
	accessKeyID, err = conf.GetString("AWS_ACCESS_KEY_ID")
	if err != nil {
		return
	}

	accessKeySecret, err = conf.GetString("AWS_SECRET_ACCESS_KEY")
	if err != nil {
		return
	}

	bucketName, err = conf.GetString("BUCKET_NAME")
	if err != nil {
		return
	}

	return
}

func getAWSSessionAndS3Client(accessKeyID string, accessKeySecret string) (sess *session.Session, client s3.S3, err error) {
	creds := credentials.NewStaticCredentials(accessKeyID, accessKeySecret, "")
	sess, err = session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: creds,
	})
	if err != nil {
		return
	}

	client = *s3.New(sess)
	return
}

// test instantiating a bucket driver with AWS creds in test.env
func TestNewBucketDriver(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	_, err = NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)
}

// test creating an auto index in an s3 bucket
func TestBucketCreateAutoIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = bd.CreateAutoIndex(indexName)
	util.Ok(t, err)

	defer bd.DropAutoIndex(indexName)

	// check that file/folder was created in bucket
	_, client, err := getAWSSessionAndS3Client(accessKeyID, accessKeySecret)
	util.Ok(t, err)

	res, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	util.Ok(t, err)

	objKeys := []string{}
	for _, item := range res.Contents {
		objKeys = append(objKeys, *item.Key)
	}

	util.Assert(t, util.StrSliceContains(indexName+"/", objKeys), "response contents contain target created index")

}

// test creating a map index in an s3 bucket
func TestBucketCreateMapIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = bd.CreateMapIndex(indexName)
	util.Ok(t, err)
	defer bd.DropMapIndex(indexName)

	// check that file/folder was created in bucket
	_, client, err := getAWSSessionAndS3Client(accessKeyID, accessKeySecret)
	util.Ok(t, err)

	res, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	util.Ok(t, err)

	objKeys := []string{}
	for _, item := range res.Contents {
		objKeys = append(objKeys, *item.Key)
	}

	util.Assert(t, util.StrSliceContains(indexName+"/", objKeys), "response contents contain target created index")
}

func TestBucketWritePageReadPage(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = bd.CreateAutoIndex(indexName)
	util.Ok(t, err)

	defer bd.DropAutoIndex(indexName)

	testVals := map[uint64]string{
		1: "hello",
		2: "world",
	}

	testKeys := []uint64{1, 2}

	const fileName = "0"

	err = bd.WritePage(testVals, testKeys, fileName, indexName)
	util.Ok(t, err)

	defer bd.DeletePage(indexName, fileName)

	vals, _, err := bd.ReadPage(fileName, indexName, 10)
	util.Ok(t, err)

	for key, val := range testVals {
		util.Equals(t, val, testVals[key])
	}

	util.Equals(t, len(testVals), len(vals))
}

func TestBucketWriteReadMapPage(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_map_index"
	err = bd.CreateMapIndex(indexName)
	util.Ok(t, err)

	defer bd.DropMapIndex(indexName)

	testVals := map[string]string{
		"1": "hello",
		"2": "world",
	}

	testKeys := []string{"1", "2"}

	const fileName = "0"

	err = bd.WriteMapPage(testVals, testKeys, fileName, indexName)
	util.Ok(t, err)

	defer bd.DeletePage(indexName, fileName)

	vals, _, err := bd.ReadMapPage(fileName, indexName, 10)
	util.Ok(t, err)

	for key, val := range testVals {
		util.Equals(t, val, testVals[key])
	}

	util.Equals(t, len(testVals), len(vals))
}

func TestBucketDriverListPages(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_index_2"
	err = bd.CreateMapIndex(indexName)
	util.Ok(t, err)

	defer bd.DropAutoIndex(indexName)

	testVals := map[string]string{
		"1": "hello",
		"2": "world",
	}

	testKeys := []string{"1", "2"}

	testFileNames := []string{"1", "2", "3", "6", "5", "4", "10", "500"}
	for _, fileName := range testFileNames {
		err = bd.WriteMapPage(testVals, testKeys, fileName, indexName)
		util.Ok(t, err)
	}

	// ascending retrieve
	pages, err := bd.ListPages(indexName, false)
	util.Ok(t, err)
	util.Equals(t, len(testFileNames), len(pages))

	lastPageID := uint64(0)
	for i, pageName := range pages {
		util.Equals(t, pageName, pages[i])
		stripped := strings.TrimSuffix(pageName, ".kb")
		pageID, err := strconv.ParseUint(stripped, 10, 64)
		util.Ok(t, err)
		util.Assert(t, pageID > lastPageID, "file list should be sorted ascending")
		lastPageID = pageID
	}

	// descending retrieve
	pagesDesc, err := bd.ListPages(indexName, true)
	util.Ok(t, err)
	util.Equals(t, len(testFileNames), len(pagesDesc))

	lastPageIDDesc := uint64(500 + 1)
	for i, pageName := range pagesDesc {
		util.Equals(t, pageName, pagesDesc[i])
		stripped := strings.TrimSuffix(pageName, ".kb")
		pageID, err := strconv.ParseUint(stripped, 10, 64)
		util.Ok(t, err)
		util.Assert(t, pageID < lastPageIDDesc, fmt.Sprintf("file list should be sorted descending: i.e. '%d' < '%d'", pageID, lastPageIDDesc))
		lastPageIDDesc = pageID
	}

}

func TestBucketDriverLockUnlockIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	longerLockDuration := toMillisDuration(500)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", longerLockDuration)
	util.Ok(t, err)

	indexName := "test_index_3"
	err = bd.CreateMapIndex(indexName)
	util.Ok(t, err)

	defer bd.DropAutoIndex(indexName)

	now := time.Now()

	err = bd.LockIndex(indexName)
	util.Ok(t, err)

	isLocked, until, err := bd.IndexIsLocked(indexName)
	util.Ok(t, err)

	util.Assert(t, isLocked, "index is not locked")
	util.Assert(t, until.After(now), "locked until TS is not after initial lock")

	err = bd.UnlockIndex(indexName)
	util.Ok(t, err)

	isLocked, _, err = bd.IndexIsLocked(indexName)
	util.Ok(t, err)
	util.Assert(t, !isLocked, "index is locked after unlock operation")
}

func TestBucketDriverErrNotExist(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	longerLockDuration := toMillisDuration(500)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", longerLockDuration)
	util.Ok(t, err)

	indexName := "test_index_notexist"

	vals, keys, err := bd.ReadPage("1", indexName, pageSize)

	util.Assert(t, IsIndexNotExist(err), "should be index-not-exist error")
	util.Equals(t, 0, len(vals))
	util.Equals(t, 0, len(keys))

}

func TestBucketDriverDropAutoIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_map_index"
	err = bd.CreateAutoIndex(indexName)
	util.Ok(t, err)

	defer bd.DropAutoIndex(indexName)

	testVals := map[uint64]string{
		1: "hello",
		2: "world",
	}

	testKeys := []uint64{1, 2}

	const fileName = "0"

	err = bd.WritePage(testVals, testKeys, fileName, indexName)
	util.Ok(t, err)

	err = bd.DropAutoIndex(indexName)
	util.Ok(t, err)

	// test files deleted
	pages, err := bd.ListPages(indexName, false)
	t.Log(pages)
	util.Equals(t, 0, len(pages))
}

func TestBucketDriverDropMapIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLockDuration)
	util.Ok(t, err)

	indexName := "test_map_index"
	err = bd.CreateMapIndex(indexName)
	util.Ok(t, err)

	defer bd.DropAutoIndex(indexName)

	testVals := map[string]string{
		"1": "hello",
		"2": "world",
	}

	testKeys := []string{"1", "2"}

	const fileName = "0"

	err = bd.WriteMapPage(testVals, testKeys, fileName, indexName)
	util.Ok(t, err)

	err = bd.DropMapIndex(indexName)
	util.Ok(t, err)

	// test files deleted
	pages, err := bd.ListPages(indexName, false)
	t.Log(pages)
	util.Equals(t, 0, len(pages))
}
