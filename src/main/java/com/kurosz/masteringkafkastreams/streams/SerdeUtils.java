package com.kurosz.masteringkafkastreams.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
class SerdeUtils {

    @Value(value = "${spring.kafka.schema-registry}")
    private String schemaRegistry;

    public <T extends SpecificRecord> Serde<T> getValueSerde() {
        final var serdeConfig = Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistry);
        final Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, false);
        return serde;
    }
}
