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
	// Kafka consts
	configPrefix                = "kafka_restore"
	configKafkaBrokers          = "kafka_brokers"
	configKafkaBrokersDelimiter = ","
	configKafkaTLSEnabled       = "kafka_tls_enabled"
	configKafkaTLSClientCert    = "kafka_tls_client_cert"
	configKafkaTLSClientKey     = "kafka_tls_client_key"
	configKafkaTLSCACert        = "kafka_tls_ca_cert"
	configRestoreTopic          = "kafka_restore_topic"
	configSourcerTopic          = "kafka_source_topic"

	// S3 consts
	configS3Endpoint        = "s3_server_endpoint"
	configS3RestoreBucket   = "s3_restore_bucket"
	configAwsSecretKey      = "s3_secret_key"
	configAwsAccesskey      = "s3_access_key"
	configAwsToken          = ""
	configStartRestoreDate  = "start_restore_date"
	configEndRestoreDate    = "end_restore_date"
	configAwsDisabledSSl    = " s3_disabled_ssl"
	configAwsForcePathStyle = "force_path_style"

	configLogDir = "logdir"
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
	// This variable is to massure runtime.
	start := time.Now()

	// This values are for debugging purposes
	viper.SetDefault(configStartRestoreDate, time.Date(2020, 04, 26, 0, 0, 0, 0, time.UTC))
	viper.SetDefault(configEndRestoreDate, time.Date(2020, 04, 27, 0, 0, 0, 0, time.UTC))
	viper.SetDefault(configKafkaBrokers, "raz-kafka.idf-cts.com:9092")
	viper.SetDefault(configKafkaTLSEnabled, false)
	viper.SetDefault(configS3Endpoint, "http://13.93.111.67:9000")
	viper.SetDefault(configS3RestoreBucket, "danielkafkatest")
	viper.SetDefault(configAwsSecretKey, "public_secret")
	viper.SetDefault(configAwsAccesskey, "public_key")
	viper.SetDefault(configRestoreTopic, "daniel_topic_test")
	viper.SetDefault(configSourcerTopic, "daniel_topic")
	viper.SetDefault(configAwsForcePathStyle, true)
	viper.SetDefault(configAwsDisabledSSl, true)

	// Set configuration auto prefix
	viper.SetEnvPrefix(configPrefix)
	viper.AutomaticEnv()
	viper.GetViper().AllowEmptyEnv(true)

	// --------- S3 config --------
	credsS3 := credentials.NewStaticCredentials(
		viper.GetString(configAwsAccesskey),
		viper.GetString(configAwsSecretKey),
		configAwsToken)

	cfgS3 := aws.NewConfig().WithRegion("us-west-1").
		WithCredentials(credsS3).
		WithEndpoint(viper.GetString(configS3Endpoint)).
		WithDisableSSL(viper.GetBool(configAwsDisabledSSl)).
		WithS3ForcePathStyle(viper.GetBool(configAwsForcePathStyle))

	sessS3 := session.New(cfgS3)

	// --------- Create Files in S3 (For Demo) --------
	// createDemoFilesInS3(sessS3, cfgS3)

	// ----------------- START TEST -------------------
	mainChan := make(chan []byte)
	filesCountChan := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(2)

	brokers := viper.GetString(configKafkaBrokers)
	topic := viper.GetString(configRestoreTopic)
	stardt := viper.GetTime(configStartRestoreDate)

	fmt.Println(brokers, topic, stardt)

	// S3-CLIENT
	go downloadDateRange(sessS3,
		viper.GetString(configS3RestoreBucket),
		viper.GetString(configSourcerTopic),
		viper.GetTime(configStartRestoreDate),
		viper.GetTime(configEndRestoreDate),
		mainChan, filesCountChan, &wg)

	// KAFKA_CLIENT
	kafkaProducer, kafkaErr := getKafkaProducer(
		strings.Split(viper.GetString(configKafkaBrokers), configKafkaBrokersDelimiter),
		viper.GetBool(configKafkaTLSEnabled),
		viper.GetString(configKafkaTLSClientCert),
		viper.GetString(configKafkaTLSClientKey),
		viper.GetString(configKafkaTLSCACert),
	)

	if kafkaErr != nil {
		WriteLog(logfileAdmin, logLevelPanic, componentKafka, kafkaErr.Error())
		panic(kafkaErr)
	}

	defer closeKafkaProducer(kafkaProducer)
	go ProcessResponse(kafkaProducer)

	dayDiff := int(viper.GetTime(configEndRestoreDate).Sub(viper.GetTime(configStartRestoreDate)).Hours() / 24)

	for day := 0; day <= dayDiff; day++ {
		filesCount := <-filesCountChan
		fmt.Println("There are: ", filesCount, "Files")
		for i := 0; i < filesCount; i++ {
			byteStream := <-mainChan
			// This loop reads the file line by line and sends it to kafka
			/* FIXME, when you read line by line, the producer stops working and the program freezes */
			for _, line := range strings.Split(string(byteStream), "\n") {
				if line == "" {
					continue
				}
				fmt.Println(line)
				message := sarama.ProducerMessage{Topic: viper.GetString(configRestoreTopic), Value: sarama.ByteEncoder(line)}
				kafkaProducer.Input() <- &message
			}
		}
	}

	wg.Wait()

	// This variable is to massure runtime.
	elapsed := time.Since(start)
	fmt.Println("Binomial took ", elapsed)
}

// closeKafkaProducer closed the kafka-producer, and prints errors if needed.
func closeKafkaProducer(producer sarama.AsyncProducer) {
	err := producer.Close()
	if err != nil {
		WriteLog(logfileAdmin, logLevelPanic, componentKafka, err.Error())
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
