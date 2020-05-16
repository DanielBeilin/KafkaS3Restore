package main

import (
	"bytes"
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
	configSourceTopic           = "kafka_source_topic"

	// S3 consts
	configS3Endpoint        = "s3_server_endpoint"
	configAwsSecretKey      = "s3_secret_key"
	configAwsAccesskey      = "s3_access_key"
	configAwsToken          = ""
	configStartRestoreDate  = "start_restore_date"
	configEndRestoreDate    = "end_restore_date"
	configAwsDisabledSSl    = " s3_disabled_ssl"
	configAwsForcePathStyle = "force_path_style"

	configLogDir = "logdir"

	configProjectName    = "project_name"
	configProjectDepType = "project_dep_type"
	configProjectSite    = "project_site"

	dnsSuffix = "dns_suffix"
)

func main() {
	WriteLog(logfileAdmin, logLevelInfo, componentMain, "Start Kafka-S3-Restore program:")

	// This variable is to massure runtime.
	start := time.Now()

	// This values are for debugging purposes
	viper.SetDefault(configStartRestoreDate, time.Date(2020, 04, 27, 0, 0, 0, 0, time.UTC))
	viper.SetDefault(configEndRestoreDate, time.Date(2020, 04, 27, 0, 0, 0, 0, time.UTC))
	viper.SetDefault(configKafkaTLSCACert, "./ssl/chain.pem")
	viper.SetDefault(configAwsForcePathStyle, true)
	viper.SetDefault(configAwsDisabledSSl, true)
	/*
	   	viper.SetDefault(configKafkaBrokers, "raz-kafka.idf-cts.com:9092")
	   	viper.SetDefault(configKafkaTLSEnabled, true)
	   	viper.SetDefault(configKafkaTLSClientCert,
	   		`-----BEGIN CERTIFICATE-----
	   MIIFazCCBFOgAwIBAgISA9k+zAgGKnZjP4uz0uxNMeB9MA0GCSqGSIb3DQEBCwUA
	   MEoxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1MZXQncyBFbmNyeXB0MSMwIQYDVQQD
	   ExpMZXQncyBFbmNyeXB0IEF1dGhvcml0eSBYMzAeFw0yMDA0MzAwNjU1MDhaFw0y
	   MDA3MjkwNjU1MDhaMCUxIzAhBgNVBAMTGmthZmthLWNsaWVudC0xLmlkZi1jdHMu
	   Y29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0tgt4WD5ul3o5I9F
	   dI5schX6GYT5H19IORM6leZbbqTcKWnf3Rhd6VroZhriDIWY6vv8EoH/pSvDSqol
	   b/A5rMythzAhbop2EQN+6iPkVWl1eZI+0ShFK+53m3bjlm+sU8Y71sXu5T/Klbo5
	   XqO/rlzZ8U7F7ThN2bbb1kSbpTeeIouE+TM4bSvH7WBqNBe6JuwEY9Ts9Uar82Fi
	   j2wy0hB5oJP70Z5HZfX3H6dXeg1hTI4YH5Cl8eKnb0of7rKXgiV8qOnO04qJxoom
	   RI88QCjayfrsx2m9QnSIdZ+0hom41XR7hqfAWtnFlKZ4ci1+DUPVxegoaho+SipT
	   XIdTOwIDAQABo4ICbjCCAmowDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsG
	   AQUFBwMBBggrBgEFBQcDAjAMBgNVHRMBAf8EAjAAMB0GA1UdDgQWBBRIqB8ssiDa
	   3PqjR9DX0rMypQetFzAfBgNVHSMEGDAWgBSoSmpjBH3duubRObemRWXv86jsoTBv
	   BggrBgEFBQcBAQRjMGEwLgYIKwYBBQUHMAGGImh0dHA6Ly9vY3NwLmludC14My5s
	   ZXRzZW5jcnlwdC5vcmcwLwYIKwYBBQUHMAKGI2h0dHA6Ly9jZXJ0LmludC14My5s
	   ZXRzZW5jcnlwdC5vcmcvMCUGA1UdEQQeMByCGmthZmthLWNsaWVudC0xLmlkZi1j
	   dHMuY29tMEwGA1UdIARFMEMwCAYGZ4EMAQIBMDcGCysGAQQBgt8TAQEBMCgwJgYI
	   KwYBBQUHAgEWGmh0dHA6Ly9jcHMubGV0c2VuY3J5cHQub3JnMIIBAwYKKwYBBAHW
	   eQIEAgSB9ASB8QDvAHUA8JWkWfIA0YJAEC0vk4iOrUv+HUfjmeHQNKawqKqOsnMA
	   AAFxyhUJ9gAABAMARjBEAiBJ7KD8oQ8S9l5z6kQppYXVebySawF3T0i44YD4C2cc
	   xwIgDmDxmJmZmndvQAoT1g2KhFflYIdJXnJ51cYcOHjFaBIAdgCyHgXMi6LNiiBO
	   h2b5K7mKJSBna9r6cOeySVMt74uQXgAAAXHKFQnuAAAEAwBHMEUCIQC6FCVoVTx2
	   7mg+KfGGKP17ZiK2ko9DkGuodLTtuC8toAIgS2xR7Ml1gClXUzKVgoplgZ2JIwez
	   wpzHrr1oNUcD21IwDQYJKoZIhvcNAQELBQADggEBAG4YVzbtWaMSFZxNwlDip0nQ
	   yjbY3NTok1hItUZl2klHsalVyIhgMPhPJ8II9W+TPOPNmipJxBND7L/SydnHcSv3
	   3ssINXRVysdKrjK9lgBdtGur1pVj9IHQP8AsF4elNS8oEixosZLTjrvsf+J4yDWh
	   axZdcx9ZpeTw1yYMIghFnQfdWvNRVJaYPW25kBPrb7oZkJiez4CaV/4GYZwfDVBd
	   t1FnA7pO6sSVu5aSdVCefU7I3fj4azsg9rqSbWvfCKa1/5jeAk62kQq53K8Fyv7S
	   +hZe1Gg8RmD9/qdYcwqsKBY/A9XVTEMkU2e1Nvuvb4+qWc4BS09/nQLSXVa9Okg=
	   -----END CERTIFICATE-----`)
	   	viper.SetDefault(configKafkaTLSClientKey,
	   		`-----BEGIN PRIVATE KEY-----
	   MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDS2C3hYPm6Xejk
	   j0V0jmxyFfoZhPkfX0g5EzqV5ltupNwpad/dGF3pWuhmGuIMhZjq+/wSgf+lK8NK
	   qiVv8DmszK2HMCFuinYRA37qI+RVaXV5kj7RKEUr7nebduOWb6xTxjvWxe7lP8qV
	   ujleo7+uXNnxTsXtOE3ZttvWRJulN54ii4T5MzhtK8ftYGo0F7om7ARj1Oz1Rqvz
	   YWKPbDLSEHmgk/vRnkdl9fcfp1d6DWFMjhgfkKXx4qdvSh/uspeCJXyo6c7TionG
	   iiZEjzxAKNrJ+uzHab1CdIh1n7SGibjVdHuGp8Ba2cWUpnhyLX4NQ9XF6ChqGj5K
	   KlNch1M7AgMBAAECggEBAKMNn12eW0HvAf5Pdg0PX3pS2JkHpojGbNoGrtXh0W+d
	   es1kHUWkZGvka7karRm172459NRzSDp5v4tsTYtloZSqrL/RTulnlqrNo0Z6/0e2
	   SKz9liq2E9hHkKPfq11Ze1FGClyrsXYEgSyNWXSA+elj0P+2RYaQdlQZ/6SSZjO7
	   ieg+U+E3Do7ZfNVxc59eVUZ0tOCpCh3xZ0nWXhB5jmssEJQhnluLHzahnHmowfRM
	   eTaNHGEWaBrtp2LE7sm0JjAXZc+/h9LBkbBI6E3apc1o21+vLtuGMpu1V1PxAap2
	   wqmV+Q2DOECqJuK8ugu8orGPz2xjNFM9L85tHUkrNmECgYEA6xIBZkvdIlLEmnw0
	   Dl6ZL43NgtcQ272hFKw8GtdTve4DgfEswCRD6M46z4pP3gsjbrrtDycn3sobN1qT
	   Cp4yeslykx1TWM9fHmuHwOMbGA46rIEZS8t9u1MOcymx7PAEVAOQY0toFV2J2QXo
	   KHhzkraFYDqXM9ufhBcQGodwl7kCgYEA5Z39S2G02J8gJocIczn9w+yVbCgWdPjA
	   v+HsWs7bk4HGIdIK79gZGv1vmDoFiY0RGJagxIbjB2DjVqlgq9WIGoSamMW4Gn+7
	   le/h70WeBuDObPS/VEEWHOX+begbdCV8PuJlEPh2Ljzp5OAbyL651hfI1ELw3AlI
	   xtl7KgqT1JMCgYEAiqYpGyw99yO4gKInlh4n3kuWXsj5UZKssuPP48kDxK/hc8BL
	   s9zwDR2uxIEBEKejM4mfj2N5+cddfC25MvcSMSgmoy0V3vkZUnj8LkIF1g92fg77
	   W6BfvaCEklqSbn03IFKl1FtCve9ZAh+gylYZXPy4+IQ2cMjmcmOkBPHD8EkCgYAz
	   44IL+OZ/VWZUjotQTriT//C8YkrA4D3entVkp/5i1R7LIcYq8TCrMr53LhV2QhSs
	   880c3EaNsk1tlhUsf7KkG3c8MuIpyte/SFhMU/UkJMVBRgW+qn6uxSK7/4nEs3vi
	   UhL4xM0gIc/RUvu0X7VrNjDCFuaLMuXpWdhFZeKGVQKBgDqvnGrPGtC8kC+4g0hr
	   pukKbOHFct+Tndv810mv9wHBIo/ofdGBYN5H3YTC9FbZiIPzvR84k/Qf5HyWq7ED
	   s4G1J2j0C6i7Ob5d2p8m1+Y8vePJFlOrjpBLUzo3vc+siq7p1vTM3EwcTj462UCy
	   75KFsrqWZJX9nkBh1XflbWl+
	   -----END PRIVATE KEY-----`)

	   	viper.SetDefault(configS3Endpoint, "http://13.93.111.67:9000")
	   	viper.SetDefault(configS3RestoreBucket, "danielkafkatest")
	   	viper.SetDefault(configAwsSecretKey, "public_secret")
	   	viper.SetDefault(configAwsAccesskey, "public_key")
	   	viper.SetDefault(configSourceTopic, "daniel_topic")
	*/
	WriteLog(logfileAdmin, logLevelInfo, componentMain, fmt.Sprintf("Start day: \t %v", viper.GetTime(configStartRestoreDate)))
	WriteLog(logfileAdmin, logLevelInfo, componentMain, fmt.Sprintf("End day:\t %v", viper.GetTime(configEndRestoreDate)))
	WriteLog(logfileAdmin, logLevelInfo, componentMain, fmt.Sprintf("Initializing configurations..."))

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
	wg.Add(1)

	fmt.Println("retriveing credentials")
	WriteLog(logfileAdmin, logLevelInfo, componentMain, fmt.Sprintf("retriveing credentials"))
	clientCert, clientKey, err := GetClientCerdentials(sessS3, viper.GetString(configProjectName), viper.GetString(configProjectSite), viper.GetString(configProjectDepType))
	if err != nil {
		WriteLog(logfileAdmin, logLevelError, componentMain, fmt.Sprintf("Error retriveing credentials"))
		panic(err)
	}
	fmt.Println("printing client cert")
	fmt.Println(clientCert)
	fmt.Println("printing client key")
	fmt.Println(clientKey)

	brokers := viper.GetString(configKafkaBrokers)
	topic := viper.GetString(configSourceTopic)
	stardt := viper.GetTime(configStartRestoreDate)
	parsedStartDate, err := time.Parse("02/01/2006", viper.GetString(configStartRestoreDate))
	if err != nil {
		fmt.Println("error formatting start restore date")
		WriteLog(logfileAdmin, logLevelError, componentMain, fmt.Sprintf("error formatting start restore date"))
	}

	parsedEndDate, err := time.Parse("02/01/2006", viper.GetString(configEndRestoreDate))
	if err != nil {
		fmt.Println("error formatting end restore date")
		WriteLog(logfileAdmin, logLevelError, componentMain, fmt.Sprintf("error formatting end restore date"))
	}

	fmt.Println(brokers, topic, stardt)
	fmt.Println(parsedStartDate, parsedEndDate)
	// The convention for the bucket name
	configS3RestoreBucket := fmt.Sprintf("%s-kafka-%s-%s-backup",
		viper.GetString(configProjectName),
		viper.GetString(configProjectDepType),
		viper.GetString(configProjectSite))

	// S3-CLIENT
	go downloadDateRange(sessS3,
		configS3RestoreBucket,
		viper.GetString(configSourceTopic),
		parsedStartDate,
		parsedEndDate,
		mainChan, filesCountChan, &wg)

	// KAFKA_CLIENT
	kafkaProducer, kafkaErr := getKafkaProducer(
		strings.Split(viper.GetString(configKafkaBrokers), configKafkaBrokersDelimiter),
		viper.GetBool(configKafkaTLSEnabled),
		clientCert,
		clientKey,
		viper.GetString(configKafkaTLSCACert),
	)

	if kafkaErr != nil {
		WriteLog(logfileAdmin, logLevelPanic, componentKafka, kafkaErr.Error())
		panic(kafkaErr)
	}

	defer closeKafkaProducer(kafkaProducer)
	go ProcessResponse(kafkaProducer)

	dayDiff := int(parsedEndDate.Sub(parsedStartDate).Hours() / 24)
	const newLineChar = byte('\n')

	WriteLog(logfileAdmin, logLevelInfo, componentMain, "Finish Initializing. Start Restore to Kafka from S3")
	for day := 0; day <= dayDiff; day++ {
		filesCount := <-filesCountChan

		WriteLog(logfileAdmin, logLevelInfo, componentMain, fmt.Sprintf("There are: %d files in day %d", filesCount, day))

		for fileIndex := 0; fileIndex < filesCount; fileIndex++ {
			byteStream := <-mainChan
			lines := bytes.Split(byteStream, []byte{newLineChar})
			WriteLog(logfileAdmin, logLevelInfo, componentMain, fmt.Sprintf("Now processing file #%d", fileIndex))

			// This loop reads the file line by line and sends it to kafka
			for _, line := range lines {
				if len(line) > 0 {
					message := sarama.ProducerMessage{Topic: fmt.Sprintf("%s-restore", viper.GetString(configSourceTopic)),
						Value: sarama.ByteEncoder(line)}
					kafkaProducer.Input() <- &message
				}
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
