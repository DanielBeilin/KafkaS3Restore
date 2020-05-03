package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Generate tls configuration from certificates files
func getTLSConfig(clientcertfile, clientkeyfile, cacertfile string) (*tls.Config, error) {
	// Load client cert
	clientcert, err := tls.LoadX509KeyPair(clientcertfile, clientkeyfile)
	if err != nil {
		return nil, err
	}

	// Load CA cert pool
	cacert, err := ioutil.ReadFile(cacertfile)
	if err != nil {
		return nil, err
	}
	cacertpool := x509.NewCertPool()
	cacertpool.AppendCertsFromPEM(cacert)

	// Generate tls config
	tlsConfig := tls.Config{}
	tlsConfig.RootCAs = cacertpool
	tlsConfig.Certificates = []tls.Certificate{clientcert}
	tlsConfig.BuildNameToCertificate()

	return &tlsConfig, nil
}

// getKafkaProducer creates new basic Kafka-producer.
func getKafkaProducer(brokers []string, tlsEnabled bool, tlsClientCert string, tlsClientKey string, tlsCACert string) (sarama.AsyncProducer, error) {
	// Create kafka producer config
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Configure tls if it's required
	if tlsEnabled {
		tlsConfig, err := getTLSConfig(tlsClientCert, tlsClientKey, tlsCACert)
		if err != nil {
			WriteLog(logfileAdmin, logLevelError, componentKafka, err.Error())
			log.Fatal(err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	return sarama.NewAsyncProducer(brokers, config)
}

// createKafkaTopic is used for tests.
func createKafkaTopic(kafkaBrokerHost string, topic string) {
	// Set broker configuration
	broker := sarama.NewBroker(kafkaBrokerHost)

	// Additional configurations. Check sarama doc for more info
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	// Open broker connection with configs defined above
	broker.Open(config)

	// check if the connection was OK
	connected, err := broker.Connected()
	if err != nil {
		log.Print(err.Error())
	}
	log.Print(connected)

	// Setup the Topic details in CreateTopicRequest struct
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(1)
	topicDetail.ReplicationFactor = int16(1)
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}

	// Send request to Broker
	response, KafkaErr := broker.CreateTopics(&request)

	// handle errors if any
	if KafkaErr != nil {
		log.Printf("%#v", &err)
	}
	t := response.TopicErrors
	for key, val := range t {
		if val.ErrMsg != nil {
			log.Println("There is an error: ", val.ErrMsg)
			WriteLog(logfileAdmin, logLevelPanic, componentKafka, val.ErrMsg)
		} else {
			log.Printf("Topic '%s' created successfully ", key)
		}
	}
	log.Printf("the response is: \t %#v", response)

	// close connection to broker
	broker.Close()
}

// ProcessResponse grabs results and errors from kafka async producer
func ProcessResponse(kafkaProducer sarama.AsyncProducer) {
	for {
		select {
		// Produce was done successfully
		case result := <-kafkaProducer.Successes():
			fmt.Println("result:", result)
		// Produce was failed
		case err := <-kafkaProducer.Errors():
			fmt.Println("err:", err)
			WriteLog(logfileAdmin, logLevelPanic, componentKafka, err.Error())
		}
	}
}
