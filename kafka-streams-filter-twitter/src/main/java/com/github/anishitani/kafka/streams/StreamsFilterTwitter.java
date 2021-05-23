package com.github.anishitani.kafka.streams;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTwitter {
    private static final Logger log = LoggerFactory.getLogger(StreamsFilterTwitter.class);

    private static final String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
    private static final String APP_ID_CONFIG = "streams-filter-twitter";

    public static void main(String[] args) {

        // create Kafka streams properties
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID_CONFIG);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> twitterTopics = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filterStream = twitterTopics.filter((key, tweet) -> twitterUserFollowersCount(tweet) > 10000);
        filterStream.to("important_tweets");

        new KafkaStreams(streamsBuilder.build(), props).start();
    }

    private static Integer twitterUserFollowersCount(String tweet) {
        try {
            return JsonParser.parseString(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception ex) {
            log.warn("Failed to parse tweet {}", tweet);
            return 0;
        }
    }
}
