package com.github.anishitani.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class TwitterConsumer {
    private static final Logger log = LoggerFactory.getLogger(TwitterConsumer.class);

    private static final String ES_USERNAME = System.getenv("ES_USERNAME");
    private static final String ES_PASSWORD = System.getenv("ES_PASSWORD");
    private static final String HOSTNAME = System.getenv("HOSTNAME");
    private static final String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
    private static final int HTTPS_PORT = 443;
    private static final String HTTPS_PROTOCOL = "https";

    private static final boolean ENABLE_AUTOCOMMIT = true;

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        // create elasticsearch client
        RestHighLevelClient esClient = createElasticSearchClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        log.info("Starting loop...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            log.info("Received {} records", records.count());

            for (ConsumerRecord<String, String> record : records) {
                Tweet tweet = mapper.readValue(record.value(), Tweet.class);
                String tweetStr = mapper.writeValueAsString(tweet);
                IndexRequest indexRequest = new IndexRequest("twitter")
                        .id(tweet.id.toString())
                        .source(tweetStr, XContentType.JSON);
                IndexResponse indexResponse = esClient.index(indexRequest, RequestOptions.DEFAULT);
                log.info("id: {} - tweet: {}", indexResponse.getId(), tweetStr);
            }

            log.info("Committing offset...");
            consumer.commitSync();
            log.info("Committed!");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Sleep failed", e);
            }
        }
    }

    public static RestHighLevelClient createElasticSearchClient() {
        Credentials basicCredential = new UsernamePasswordCredentials(ES_USERNAME, ES_PASSWORD);
        CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY, basicCredential);

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(HOSTNAME, HTTPS_PORT, HTTPS_PROTOCOL))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialProvider));
        return new RestHighLevelClient(clientBuilder);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "twitter-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        if (ENABLE_AUTOCOMMIT) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("twitter_tweets"));

        return consumer;
    }
}
