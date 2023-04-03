package com.kurosz.masteringkafkastreams.utils;

import com.kurosz.masteringkafkastreams.config.KafkaConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Collections;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@TestConfiguration
public class KafkaTestUtils {

    @Autowired
    private KafkaConfig kafkaConfig;

    public <T extends SpecificRecord> Producer<String, T> getProducer(EmbeddedKafkaBroker embeddedKafkaBroker) {
        Serde<T> serde = getValueSerde();

        var producerProps = org.springframework.kafka.test.utils.KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serde.serializer().getClass());
        producerProps.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.getSchemaRegistry());

        return new DefaultKafkaProducerFactory<String, T>(producerProps).createProducer();
    }

    public <T extends SpecificRecord> Consumer<String, T> getConsumer(EmbeddedKafkaBroker embeddedKafkaBroker, String group) {
        Serde<T> serde = getValueSerde();

        var consumerProps = org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps(group, "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, serde.deserializer().getClass());
        consumerProps.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.getSchemaRegistry());

        return new DefaultKafkaConsumerFactory<String, T>(consumerProps).createConsumer();
    }

    public  Producer<String, String> getStringProducer(EmbeddedKafkaBroker embeddedKafkaBroker) {
        var producerProps = org.springframework.kafka.test.utils.KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.getSchemaRegistry());

        return new DefaultKafkaProducerFactory<String, String>(producerProps).createProducer();
    }

    public  Consumer<String, String> getStringConsumer(EmbeddedKafkaBroker embeddedKafkaBroker, String group) {
        var consumerProps = org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps(group, "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.getSchemaRegistry());

        return new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
    }

    public Consumer<String, Double> getDoubleConsumer(EmbeddedKafkaBroker embeddedKafkaBroker, String group) {
        var consumerProps = org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps(group, "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class);
        consumerProps.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.getSchemaRegistry());

        return new DefaultKafkaConsumerFactory<String, Double>(consumerProps).createConsumer();
    }

    private <T extends SpecificRecord> Serde<T> getValueSerde() {
        final Map<String, String> serdeConfig = Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.getSchemaRegistry());
        Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, false);
        return serde;
    }
}
