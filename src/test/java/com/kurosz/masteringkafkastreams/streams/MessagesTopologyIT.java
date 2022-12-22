package com.kurosz.masteringkafkastreams.streams;

import com.kurosz.masteringkafkastreams.avro.Tweet;
import com.kurosz.masteringkafkastreams.avro.TweetLang;
import com.kurosz.masteringkafkastreams.config.KafkaTopologyConfig;
import com.kurosz.masteringkafkastreams.utils.TestKafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9091", "port=9091"
        }
        ,
        topics = {
                "tweets-input",
                "tweets-output"
        }
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(TestKafkaConfig.class)
class MessagesTopologyIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private TestKafkaConfig testKafkaConfig;

    @Autowired
    private KafkaTopologyConfig topologyConfig;

    Producer<String, Tweet> producer;

    Consumer<String, Tweet> consumer;


    @BeforeAll
    public void init() {
        producer = testKafkaConfig.getProducer(embeddedKafkaBroker);
        consumer = testKafkaConfig.getConsumer(embeddedKafkaBroker, "consumer-test-group");

        consumer.subscribe(Collections.singleton(topologyConfig.tweetsOutput()));
    }

    @AfterAll
    public void close() {
        producer.flush();
        producer.close();

        consumer.close();
    }

    @Test
    public void shouldSendAvroMessage() {
        var tweet = new Tweet();
        tweet.setText("hello tweet");
        tweet.setLang(TweetLang.EN);
        producer.send(new ProducerRecord<>(topologyConfig.tweetsInput(), "key", tweet));

        var record = KafkaTestUtils.getSingleRecord(consumer, "tweets-output", Duration.ofSeconds(5));

        assertEquals(tweet, record.value());

    }
}