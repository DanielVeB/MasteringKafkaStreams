package com.kurosz.masteringkafkastreams.streams;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

@TestConfiguration
public class KafkaTestConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.schema-registry}")
    private String schemaRegistry;

    private final AtomicInteger integer = new AtomicInteger();

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    @Primary
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "streams-app-" + integer.getAndIncrement());
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        props.put(COMMIT_INTERVAL_MS_CONFIG, 1000);

        return new KafkaStreamsConfiguration(props);
    }

    public String getSchemaRegistry() {
        return schemaRegistry;
    }

}
