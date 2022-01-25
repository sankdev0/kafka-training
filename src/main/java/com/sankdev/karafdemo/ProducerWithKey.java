package com.sankdev.karafdemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * This class produces one message with key. Such messages get stored in the same partition and behave synced.
 */
public class ProducerWithKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);

        // Configure the producer
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producerOne = new KafkaProducer<>(properties);

        // Sending the data to topic
        // Note: to read the message you need to have an active consumer (or read topic --from-beginning)
        for (int i = 0; i < 10; i++) {
            String topic = "test.one";
            String key = "id_" + i;
            String value = "Test message " + i;
            logger.info("===>>> key " + key);

            // Create the producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producerOne.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception == null) {
                    logger.info("The details of the record are: \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Getting error", exception);
                }
            }).get(); // this sends synchronous data forcefully and BLOCKING
        }
        producerOne.flush();
        producerOne.close();
    }
}
