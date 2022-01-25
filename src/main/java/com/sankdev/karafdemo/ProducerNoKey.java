package com.sankdev.karafdemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This class produces one message without key. Such messages get stored in random partitions and behave asynchronously.
 */
public class ProducerNoKey {

    public static void main(String[] args) {

        // Configure the producer
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producerOne = new KafkaProducer<>(properties);

        // Create the producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("test.one", "Hi Kafka!");

        // Sending the data to topic
        // Note: to read the message you need to have an active consumer (or read topic --from-beginning)
        producerOne.send(record, (RecordMetadata metadata, Exception exception) -> {
            Logger logger = LoggerFactory.getLogger(ProducerNoKey.class);
            if (exception == null) {
                logger.info("The details of the record are: \n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp());
            } else {
                logger.error("Getting error", exception);
            }
        });
        producerOne.flush();
        producerOne.close();
    }
}
