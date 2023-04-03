package com.kurosz.masteringkafkastreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka.topology")
public record KafkaTopologyConfig(
        String tweetsInput,
        String tweetsOutput,

        String usersInput,

        String usersOutput
) {
}
