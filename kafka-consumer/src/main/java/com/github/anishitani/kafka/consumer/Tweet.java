package com.github.anishitani.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {
    @JsonProperty("id")
    Long id;

    @JsonProperty("timestamp_ms")
    Date timestamp;

    @JsonProperty("text")
    String text;

    @JsonProperty(value = "user",access = JsonProperty.Access.WRITE_ONLY)
    User user;

    @JsonUnwrapped(prefix = "user.")
    @JsonProperty(value = "user", access = JsonProperty.Access.READ_ONLY)
    public User getUser() {
        return user;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public class User {
        @JsonProperty("name")
        String userName;
        @JsonProperty("id")
        Long userId;
    }
}
