package com.unify.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world");

        //send data
        producer.send(record);

        //Flush data
        producer.flush();

        //Close producer
        producer.close();

    }
}
