package org.example;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {

        RestHighLevelClient restHighLevelClient = createOpenSearchClient();
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        try (restHighLevelClient; kafkaConsumer) {
            GetIndexRequest getIndexRequest = new GetIndexRequest("wikimedia");
            boolean indexExists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                log.info("The wikimedia index already exists.");
            }
            String topic = "wikimedia.change";
            kafkaConsumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records =
                        kafkaConsumer.poll(Duration.ofMillis(100));

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON);

                    IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                    bulkRequest.add(indexRequest);
                    log.info(index.getId());
                }

                if (bulkRequest.numberOfActions() != 0) {
                    restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                }
            }

        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "172.17.241.204:9092";
        String groupId = "consumer-opensearch-group-id";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String connectionString = "http://localhost:9200";
        String username = "admin"; // твой логин
        String password = "MyS3cretPass!"; // твой пароль

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(username, password)
        );

        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(connectionString))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        )
        );
    }
}