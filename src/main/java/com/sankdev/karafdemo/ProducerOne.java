package com.sankdev.karafdemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerOne {

    public static void main(String[] args) {

        // Configure the producer
        String bootstrapServers="127.0.0.1:9092";
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
        producerOne.send(record);
        producerOne.flush();
        producerOne.close();
    }
}
