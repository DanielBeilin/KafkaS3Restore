package main

import (
	"sync"
	"time"

	"fmt"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

const (
	MinioEndpoint      = "bdservices-minio.idf-cts.com:9000"
	KafkaEndpoint      = "raz-kafka.idf-cts.com:9092"
	AwsAccessKeyID     = "public_key"
	AwsSecretAccessKey = "public_secret"
	Token              = ""
	Topic              = "test_topic"
)

func main() {
	// --------- S3 config --------
	credsS3 := credentials.NewStaticCredentials(AwsAccessKeyID, AwsSecretAccessKey, Token)
	cfgS3 := aws.NewConfig().WithRegion("us-west-1").WithCredentials(credsS3).WithEndpoint(MinioEndpoint).WithDisableSSL(true).WithS3ForcePathStyle(true)
	sessS3 := session.New(cfgS3)

	// --------- Create Files in S3 (For Demo) --------
	// createDemoFilesInS3(sessS3, cfgS3)

	// ----------------- START TEST -------------------
	mainChan := make(chan []byte)
	filesCountChan := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(1)

	// S3-CLIENT
	go downloadDateRange(sessS3, "connect", "test_topic", time.Now().AddDate(0, 0, -3), time.Now(), mainChan, filesCountChan, &wg)

	// KAFKA_CLIENT
	producer, err := getKafkaProducer([]string{KafkaEndpoint}, false, "", "", "")

	if err != nil {
		panic(err)
	}

	defer closeKafkaProducer(producer)
	go ProcessResponse(producer)

	for day := 0; day < 4; day++ {
		filesCount := <-filesCountChan
		fmt.Println("There are: ", filesCount, "Files")
		for i := 0; i < filesCount; i++ {
			msg := <-mainChan
			message := sarama.ProducerMessage{Topic: Topic, Value: sarama.ByteEncoder(msg)}
			producer.Input() <- &message
		}
	}

	wg.Wait()
}

// closeKafkaProducer closed the kafka-producer, and prints errors if needed.
func closeKafkaProducer(producer sarama.AsyncProducer) {
	err := producer.Close()
	if err != nil {
		fmt.Println("Error closing producer: ", err)
		return
	}
	fmt.Println("Producer closed")
}

// createDemoFilesInS3 is used for create demo files in S3, Only for tests.
func createDemoFilesInS3(sessS3 *session.Session, cfgS3 *aws.Config) {
	const FileBasicName string = "logs"
	const Message1 string = "This is messasge #1"
	const Message2 string = "This is messasge #2"
	for i := 0; i <= 10; i++ {
		demoDate := time.Now().AddDate(0, 0, -10+i)
		writeFile(fmt.Sprintf("%s-%d.txt", FileBasicName, i),
			fmt.Sprintf("{\"timestamp\":\"%s 11:29:11,644\",\"level\":\"INFO\",\"logger\":\"kafka.server.KafkaServer\",\"thread\":\"main\",\"message\":\"%s\"}\n{\"timestamp\":\"%s 11:29:11,870\",\"level\":\"INFO\",\"logger\":\"kafka.server.KafkaServer\",\"thread\":\"main\",\"message\":\"%s\"})", demoDate, Message1, demoDate, Message2))
		AddFileToS3(sessS3, cfgS3, fmt.Sprintf("%s-%d.txt", FileBasicName, i), "connect", Topic, demoDate)
	}
}
