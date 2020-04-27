 package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/viper"
)

const (
	configPrefix                = "kafka_restore"
	configKafkaBrokers          = "kafka_brokers"
	configKafkaBrokersDelimiter = ","
	configKafkaTLSEnabled       = "kafka_tls_enabled"
	configKafkaTLSClientCert    = "kafka_tls_client_cert"
	configKafkaTLSClientKey     = "kafka_tls_client_key"
	configKafkaTLSCACert        = "kafka_tls_ca_cert"
	configS3Endpoint            = "s3_server_endpoint"
	configS3RestoreBucket       = "s3_restore_bucket"
	configAwsSecretKey          = "s3_secret_key"
	configAwsAccesskey          = "s3_access_key"
	configAwsToken              = ""
	configLogDir                = "logdir"
	configRestoreTopic          = "kafka_restore_topic"
)

/*const (
	MinioEndpoint      = "bdservices-minio.idf-cts.com:9000"
	KafkaEndpoint      = "raz-kafka.idf-cts.com:9092"
	AwsAccessKeyID     = "public_key"
	AwsSecretAccessKey = "public_secret"
	Token              = ""
	Topic              = "test_topic"
)*/

func main() {
	// Set configuration auto prefix
	viper.SetEnvPrefix(configPrefix)
	viper.AutomaticEnv()
	viper.GetViper().AllowEmptyEnv(true)

	// --------- S3 config --------
	credsS3 := credentials.NewStaticCredentials(viper.GetString(configAwsAccesskey),
		viper.GetString(configAwsSecretKey),
		viper.GetString(configAwsToken))

	cfgS3 := aws.NewConfig().WithRegion("us-west-1").
		WithCredentials(credsS3).
		WithEndpoint(viper.GetString(configS3Endpoint)).
		WithDisableSSL(true).WithS3ForcePathStyle(true)

	sessS3 := session.New(cfgS3)

	// --------- Create Files in S3 (For Demo) --------
	// createDemoFilesInS3(sessS3, cfgS3)

	// ----------------- START TEST -------------------
	mainChan := make(chan []byte)
	filesCountChan := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(1)

	// S3-CLIENT
	go downloadDateRange(sessS3,
		viper.GetString(configS3RestoreBucket),
		viper.GetString(configRestoreTopic),
		time.Now().AddDate(0, 0, -3),
		time.Now(), mainChan, filesCountChan, &wg)

	// KAFKA_CLIENT
	kafkaProducer, kafkaErr := getKafkaProducer(
		strings.Split(viper.GetString(configKafkaBrokers), configKafkaBrokersDelimiter),
		viper.GetBool(configKafkaTLSEnabled),
		viper.GetString(configKafkaTLSClientCert),
		viper.GetString(configKafkaTLSClientKey),
		viper.GetString(configKafkaTLSCACert),
	)

	if kafkaErr != nil {
		// TODO write error to log file
		panic(kafkaErr)
	}

	defer closeKafkaProducer(kafkaProducer)
	go ProcessResponse(kafkaProducer)

	for day := 0; day < 4; day++ {
		filesCount := <-filesCountChan
		fmt.Println("There are: ", filesCount, "Files")
		for i := 0; i < filesCount; i++ {
			msg := <-mainChan
			message := sarama.ProducerMessage{Topic: viper.GetString(configRestoreTopic), Value: sarama.ByteEncoder(msg)}
			kafkaProducer.Input() <- &message
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

/*
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
*/
