FROM centos
EXPOSE 9092
RUN mkdir /kafkaS3Restore 
ADD ./ssl /kafkaS3Restore/ssl
ADD ./kafkaS3Restore /kafkaS3Restore/kafkaS3Restore
RUN chmod +x /kafkaS3Restore/kafkaS3Restore