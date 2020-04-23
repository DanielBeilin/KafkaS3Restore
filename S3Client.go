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

// listObjectsForDate: returns a list of S3-objects in a certin date
func listObjectsForDate(s3Session *s3.S3, bucket string, topic string, date string) ([]*s3.Object, error) {
	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fmt.Sprintf("%s/%s", topic, date)),
	}

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
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return nil, err
	}
	return result.Contents, nil
}

// downloadDateRange: Gets start and end dates and downloads all the files in this ranges from S3 to chanel-buffer
func downloadDateRange(s3Session *session.Session, bucket string, topic string, start time.Time, end time.Time, dataChan chan []byte, filesCountChan chan int, wg *sync.WaitGroup) {
	WriteLog(logfileS3, logLevelInfo, componentKafka, fmt.Sprintf("downloadDateRange: StartDate-%s, EndTime %s", start, end))
	s3Downloader := s3manager.NewDownloader(s3Session)

	// Run for all the range (include the 'end' day)
	for end.After(start) || end.Equal(start) {
		objectList, err := listObjectsForDate(s3.New(s3Session), bucket, topic, string(start.Format("Year=2006/Month=01/Day=02")))
		if err != nil {
			panic(err)
		}

		// Send the count of files in the current day to the chanel.
		filesCountChan <- len(objectList)

		downloadObjectList(s3Downloader, bucket, objectList, dataChan)

		// Go to the next day.
		start = start.AddDate(0, 0, 1)
	}

	wg.Done()
}

// downloadObjectList: Sends the every file in the S3 directory to the chanel as buffer.
func downloadObjectList(s3Downloader *s3manager.Downloader, bucket string, objectsToDownload []*s3.Object, fileChan chan []byte) {
	buffer := aws.NewWriteAtBuffer([]byte{})
	for _, element := range objectsToDownload {

		WriteLog(logfileS3, logLevelInfo, componentS3, fmt.Sprintf("Download: \t%s\t to buffer", *element.Key))

		// Download file content to buffer
		_, err := s3Downloader.Download(buffer, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(*element.Key),
		})
		if err != nil {
			panic(err)
		} else {
			// Write buffer to the chanel
			fileChan <- buffer.Bytes()
		}
	}
}

// AddFileToS3 Writes a file to S3. This Function is for initialize tests only.
func AddFileToS3(s *session.Session, cfg *aws.Config, localFilePath string, s3Bucket string, topic string, time time.Time) {

	// Open local file
	f, err := os.Open(localFilePath)
	if err != nil {
		fmt.Println("Error while open local file !", err)
	}

	defer f.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := f.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	f.Read(buffer)

	// Generate the name of the S3 file according its topic and name.
	key := fmt.Sprintf("/%s/%s/%s", topic, string(time.Format("Year=2006/Month=01/Day=02")), f.Name())

	putFileOutput, err := s3.New(s, cfg).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String("connect"),
		Key:                aws.String(key),
		Body:               bytes.NewReader(buffer),
		ContentLength:      aws.Int64(size),
		ContentType:        aws.String(http.DetectContentType(buffer)),
		ContentDisposition: aws.String("attachment"),
	})

	if err != nil {
		WriteLog(logfileS3, logLevelError, componentS3, fmt.Sprintf("Error when put the file in S3:\t\t %v", err))
	}

	WriteLog(logfileS3, logLevelInfo, componentS3, fmt.Sprintf("Put file successfully: \t\t%v", putFileOutput))
}
