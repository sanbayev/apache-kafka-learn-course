package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) {

        String bootstrapServers = "172.17.241.204:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10");
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "first_topic";
                String value = "hello world " + i;
                String key = "id_" + i;
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                    } else {
                        log.error("Error while producing", e);
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();
        producer.close();
    }
}
