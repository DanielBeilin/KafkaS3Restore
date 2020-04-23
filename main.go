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
	KafkaTopic         = "test_topic"
	StartDateStr       = "17-04-2020"
	EndDateStr         = "22-04-2020"
)

func main() {
	WriteLog(logfileMain, logLevelInfo, componentKafka, "Start program")

	// Caculate the days we need to restore
	startDate, _ := time.Parse("02-01-2006", StartDateStr)
	endDate, _ := time.Parse("02-01-2006", EndDateStr)
	daysToRestore := int(endDate.Sub(startDate).Hours()/24) + 1

	// Config S3 storage.
	credsS3 := credentials.NewStaticCredentials(AwsAccessKeyID, AwsSecretAccessKey, Token)
	cfgS3 := aws.NewConfig().WithRegion("us-west-1").WithCredentials(credsS3).WithEndpoint(MinioEndpoint).WithDisableSSL(true).WithS3ForcePathStyle(true)
	sessS3 := session.New(cfgS3)

	// --------- Create Files in S3 (For Demo) --------
	// createDemoFilesInS3(sessS3, cfgS3)

	// ----------------- START TEST -------------------
	dataChan := make(chan []byte)
	filesCountChan := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(1)

	// // Get files from S3 to chanel as buffer
	go downloadDateRange(sessS3, "connect", KafkaTopic, startDate, endDate, dataChan, filesCountChan, &wg)

	// Initialize Kafka producer
	producer, err := getKafkaProducer([]string{KafkaEndpoint}, false, "", "", "")

	if err != nil {
		WriteLog(logfileKafka, logLevelError, componentKafka, fmt.Sprintf("Error in getting producer: %s", err))
		panic(err)
	}

	// Close Kafka producer when program finish.
	defer closeKafkaProducer(producer)

	// Monitor the Kafka responses
	go ProcessResponse(producer)

	for day := 0; day < daysToRestore; day++ {

		// Get the number of files in the current day.
		filesCount := <-filesCountChan
		WriteLog(logfileMain, logLevelInfo, componentS3, fmt.Sprintf("There are: %d files in date: %s", filesCount, startDate.AddDate(0, 0, day).Format("2006-01-02")))
		fmt.Printf("There are: %d files in date: %s", filesCount, startDate.AddDate(0, 0, day).Format("2006-01-02"))

		for fileIndex := 0; fileIndex < filesCount; fileIndex++ {
			msg := <-dataChan
			message := sarama.ProducerMessage{Topic: KafkaTopic, Value: sarama.ByteEncoder(msg)}
			producer.Input() <- &message
		}
	}

	fmt.Println("~~~ !!! FINISH !!! ~~~")
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
		AddFileToS3(sessS3, cfgS3, fmt.Sprintf("%s-%d.txt", FileBasicName, i), "connect", KafkaTopic, demoDate)
	}
}
