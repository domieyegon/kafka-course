package com.unify;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {


        String bootstrapServers = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<=10; i++) {
            //Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world "+ i);

            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes everytime whenever a message is successfully sent or an exception is thrown
                    if (e == null) {

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss:S");
                        logger.info("Received new metadata.\n Topic: {}\n Partition: {}\n Offset: {}\n Timestamp: {} ",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), simpleDateFormat.format(recordMetadata.timestamp()));
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        //Flush data
        producer.flush();

        //Close producer
        producer.close();

    }
}
