package main

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"bytes"
	"fmt"
	"net/http"
	"os"
	"time"
)

// function that returns a list of objects in a certin date
func listObjectsForDate(s3Session *s3.S3, bucket string, topic string, date string) ([]*s3.Object, error) {
	fmt.Println("Listing objects")
	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fmt.Sprintf("topics/%s/%s", topic, date)),
	}
	fmt.Println("AFTER LIST S3")
	result, err := s3Session.ListObjects(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			WriteLog(logfileAdmin, logLevelPanic, componentS3, err.Error())
			fmt.Println(err.Error())
		}
		return nil, err
	}

	return result.Contents, nil
}

// Download all objects between a given start date and end date
// move to main.go
func downloadDateRange(s3Session *session.Session, bucket string, topic string, start time.Time, end time.Time, mainChan chan []byte, filesCountChan chan int, wg *sync.WaitGroup) {
	WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("Start downloadDateRange from %v to %v", start, end))
	s3Downloader := s3manager.NewDownloader(s3Session)
	for end.After(start) || end.Equal(start) {
		WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("Downloadging files for day %v", start))
		objectList, err := listObjectsForDate(s3.New(s3Session), bucket, topic, string(start.Format("year=2006/month=01/day=02")))
		filesCountChan <- len(objectList)
		if err != nil {
			WriteLog(logfileAdmin, logLevelPanic, componentS3, err.Error())
			fmt.Println(err.Error())
			panic(err)
		} else {
			fmt.Println("OK")
		}

		downloadObjectList(s3Downloader, bucket, objectList, mainChan)

		WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("Finish to download files for day %v", start))
		start = start.AddDate(0, 0, 1)
	}

	WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("Finish to download files from S3"))
	wg.Done()
}

// returns a buffer
// TODO check if buffer empties for each day
func downloadObjectList(s3Downloader *s3manager.Downloader, bucket string, objectsToDownload []*s3.Object, mainChan chan []byte) {
	WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("Start downloadObjectList"))
	buffer := aws.NewWriteAtBuffer([]byte{})
	for _, element := range objectsToDownload {
		_, err := s3Downloader.Download(buffer, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(*element.Key),
		})
		if err != nil {
			WriteLog(logfileAdmin, logLevelPanic, componentS3, err.Error())
			panic(err)
		} else {

			WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("Write buffer to chanel"))
			mainChan <- buffer.Bytes()
		}
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// AddFileToS3 takes in a session, fileDir, s3_Bucket, and s3_Dir_Path
// In this case fileDir is the local file to read in and upload to S3; in this case we expect it to be local to the code
// S3 Dir Path is the directory within the S3 Bucket to write to
func AddFileToS3(s *session.Session, cfg *aws.Config, localFilePath string, s3Bucket string, topic string, time time.Time) error {

	f, err := os.Open(localFilePath)
	if err != nil {
		WriteLog(logfileAdmin, logLevelPanic, componentS3, err.Error())
		fmt.Println("Error while opening local file !", err)
	}

	defer f.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := f.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	f.Read(buffer)

	key := fmt.Sprintf("/%s/%s/%s", topic, string(time.Format("Year=2006/Month=01/Day=02")), f.Name())

	pufFileOutput, err := s3.New(s, cfg).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String("connect"),
		Key:                aws.String(key),
		Body:               bytes.NewReader(buffer),
		ContentLength:      aws.Int64(size),
		ContentType:        aws.String(http.DetectContentType(buffer)),
		ContentDisposition: aws.String("attachment"),
	})

	if err != nil {
		WriteLog(logfileAdmin, logLevelPanic, componentS3, err.Error())
		panic(err)
	}

	fmt.Printf("\n\n--pufFileOutput: %v\n--ERR: %v\n", pufFileOutput, err)

	return err
}

// GetClientCerdentials returns the cert and key for a given project
func GetClientCerdentials(s3Session *session.Session, projectName string, projectSite string, projectDepType string) (ClientCert, ClientKey []byte, err error) {
	s3Downloader := s3manager.NewDownloader(s3Session)
	// The path to the certificates is bucket/Component/projectName/Site/Deployment/
	path := fmt.Sprintf("kafka/%s/%s/client", projectDepType,projectSite)

	buffer := aws.NewWriteAtBuffer([]byte{})
	WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("retriveing client certificate"))
	_, ClientError := s3Downloader.Download(buffer, &s3.GetObjectInput{
		Bucket: aws.String(projectName),
		Key:    aws.String(fmt.Sprintf("%s/%s-kafka-%s-%s-client.%s.pem", path,projectName,projectDepType,projectSite,dnsSuffix)),
	})
	if ClientError != nil {
		return nil, nil, ClientError
	}
	ClientCertString := buffer.Bytes()

	buffer = aws.NewWriteAtBuffer([]byte{})
	WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("retriveing client key"))
	_, KeyError := s3Downloader.Download(buffer, &s3.GetObjectInput{
		Bucket: aws.String(projectName),
		Key:    aws.String(fmt.Sprintf("%s/%s-kafka-%s-%s-client.%s.key", path,projectName,projectDepType,projectSite,dnsSuffix)),
	})
	if ClientError != nil {
		return nil, nil, KeyError
	}
	ClientKeyString := buffer.Bytes()

	WriteLog(logfileAdmin, logLevelInfo, componentS3, fmt.Sprintf("retrived credentials successfully"))
	return ClientCertString, ClientKeyString, nil
}
  