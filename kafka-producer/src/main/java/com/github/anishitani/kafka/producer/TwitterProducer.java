package com.github.anishitani.kafka.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TwitterProducer {

    private static final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    private static final boolean ENABLE_SAFE_PRODUCER = false;
    private static final boolean ENABLE_HIGH_THROUGHPUT = true;


    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String secret;
    private final String bootstrapServer;
    private final List<String> topics;

    public TwitterProducer() {
        this.consumerKey = System.getenv("CONSUMER_KEY");
        this.consumerSecret = System.getenv("CONSUMER_SECRET");
        this.token = System.getenv("TOKEN");
        this.secret = System.getenv("SECRET");
        this.bootstrapServer = System.getenv("BOOTSTRAP_SERVER");
        this.topics = Arrays.stream(Optional.ofNullable(System.getenv("TWITTER_TOPICS"))
                .orElseThrow(() -> new RuntimeException("Please set env var TWITTER_TOPICS=topicA,topicB,etc."))
                .split(","))
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // create shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down application...");
            log.info("shutting down twitter client...");
            twitterClient.stop();
            log.info("shutting down kafka producer...");
            producer.close();
            log.info("done");
        }));

        // loop to send tweets to kafka
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Failed to fetch tweets", e);
                twitterClient.stop();
            }

            if (msg != null) {
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", msg));
            } else {
                log.info("No messages found!");
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (ENABLE_SAFE_PRODUCER) {
            props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            // retries and max_in_flight are correct for version 2.7
        }

        if (ENABLE_HIGH_THROUGHPUT) {
            props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
            props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
            // retries and max_in_flight are correct for version 2.7
        }

        return new KafkaProducer<>(props);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(topics);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        return new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();
    }
}
