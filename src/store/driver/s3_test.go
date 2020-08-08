package driver_test

import (
	"keybite/config"
	"keybite/store/driver"
	"keybite/util"
	"testing"

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

	_, err = driver.NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLogDuration)
	util.Ok(t, err)
}

// test creating an auto index in an s3 bucket
func TestBucketCreateAutoIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := driver.NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLogDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = bd.CreateAutoIndex(indexName)
	util.Ok(t, err)

	// check that file/folder was created in bucket
	_, client, err := getAWSSessionAndS3Client(accessKeyID, accessKeySecret)
	util.Ok(t, err)

	res, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	util.Ok(t, err)

	util.Equals(t, 2, len(res.Contents))
}

// test creating a map index in an s3 bucket
func TestBucketCreateMapIndex(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := driver.NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLogDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = bd.CreateMapIndex(indexName)
	util.Ok(t, err)
	defer bd.DeleteIndex(indexName)

	// check that file/folder was created in bucket
	_, client, err := getAWSSessionAndS3Client(accessKeyID, accessKeySecret)
	util.Ok(t, err)

	res, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	util.Ok(t, err)

	t.Log(res.Contents)
	util.Equals(t, 1, len(res.Contents))
}

func TestBucketWritePageReadPage(t *testing.T) {
	accessKeyID, accessKeySecret, bucketName, err := getEnvCreds()
	util.Ok(t, err)

	bd, err := driver.NewBucketDriver(pageExtension, bucketName, accessKeyID, accessKeySecret, "", testLogDuration)
	util.Ok(t, err)

	indexName := "test_index"
	err = bd.CreateAutoIndex(indexName)
	util.Ok(t, err)

	defer bd.DeleteIndex(indexName)

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

	t.Log(vals)
	t.Log(testVals)
	// t.FailNow()

	for key, val := range testVals {
		util.Equals(t, val, testVals[key])
	}

	util.Equals(t, len(testVals), len(vals))
}
