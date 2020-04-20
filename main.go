package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"fmt"
)

func main() {
	const (
		Endpoint           = "bdservices-minio.idf-cts.com:9000/"
		AwsAccessKeyID     = "public_key"
		AwsSecretAccessKey = "public_secret"
		Token              = ""
	)

	creds := credentials.NewStaticCredentials(AwsAccessKeyID, AwsSecretAccessKey, Token)

	// Region: 				Require not null, I don't know why
	// DislableSSL: 	 	Minio is in http right now
	// S3ForcePathStyle: 	Use this format to add the bucket: http://ENDPOINT/BUCKET/KEY and not http://BUCKET.ENDPOINT/KEY
	cfg := aws.NewConfig().WithRegion("us-west-1").WithCredentials(creds).WithEndpoint(Endpoint).WithDisableSSL(true).WithS3ForcePathStyle(true)
	svc := s3.New(session.New(), cfg)

	a, err := listObjectsForDate(svc, "connect", "")
	if err == nil {
		fmt.Printf("FILES: %v", a)
	}

}
