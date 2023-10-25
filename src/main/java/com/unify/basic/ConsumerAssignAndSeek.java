package com.unify.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignAndSeek {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-app";
        String offsetReset = "earliest";
        String topic = "first-topic";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        //Create consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            // Assign and seek are mostly used  to replay data or fetch a specific message

            // Assign
            TopicPartition  topicPartitionToReadFrom =  new TopicPartition(topic, 0);
            consumer.assign(Collections.singleton(topicPartitionToReadFrom));

            //Seek
            long offsetToReadFrom = 15L;
            consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

            int numberOfMessagesToRead = 5;
            int numberOfMessagesRead = 0;
            boolean keepOnReading = true;
            //poll for new data
            while (keepOnReading) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: {}, value: {}", record.key(), record.value());
                    logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    numberOfMessagesRead++;

                    if (numberOfMessagesRead >= numberOfMessagesToRead){
                        keepOnReading =false;
                        break;
                    }
                }
            }

            logger.info("Exiting the application");
        }

    }
}
