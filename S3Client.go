package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"fmt"
	"os"
	"time"
)

// function that returns a list of objects in a certin date
func listObjectsForDate(s3Session *s3.S3, bucket string, date string) ([]*s3.Object, error) {

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		// Prefix: aws.String(fmt.Sprintf("root/%s", date)),
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

// Download all objects between a given start date and end date
// TODO move to main.go
func downloadDateRange(s3Session *session.Session, bucket string, start time.Time, end time.Time) {

	buffer := aws.NewWriteAtBuffer([]byte{})
	s3Downloader := s3manager.NewDownloader(s3Session)

	for !start.Equal(end) {
		objectList, err := listObjectsForDate(s3.New(s3Session), bucket, string(start.Format("2006/01/02")))
		if err != nil {
			// TODO implement write to log
			panic(err)
		}

		// downloadObjectList(buffer, s3Downloader, bucket, objectList)
		for _, element := range objectList {
			_, err := s3Downloader.Download(buffer, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(*element.Key),
			})
			if err != nil {
				// TODO implement write to log
				panic(err)
			}
		}

		start.AddDate(0, 0, 1)
	}
}

// returns a buffer
func downloadObjectList(buffer *aws.WriteAtBuffer, s3Downloader *s3manager.Downloader, bucket string, objectsToDownload []*s3.Object) *aws.WriteAtBuffer {
	for _, element := range objectsToDownload {
		_, err := s3Downloader.Download(buffer, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(*element.Key),
		})
		if err != nil {
			// TODO implement write to log
			panic(err)
		}
	}

	return buffer
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
