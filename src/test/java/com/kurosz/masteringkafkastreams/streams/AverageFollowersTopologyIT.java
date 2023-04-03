package com.kurosz.masteringkafkastreams.streams;

import com.kurosz.masteringkafkastreams.avro.User;
import com.kurosz.masteringkafkastreams.config.KafkaTopologyConfig;
import com.kurosz.masteringkafkastreams.utils.KafkaTestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.collections.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest()
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@EmbeddedKafka(
        partitions = 3,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092"
        },
        topics = {
                "tweets-input",
                "tweets-output",
                "users-input",
                "users-output"
        }
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import({KafkaTestUtils.class, KafkaTestConfig.class})
class AverageFollowersTopologyIT {
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTestUtils kafkaTestUtils;

    @Autowired
    private KafkaTopologyConfig topologyConfig;

    Producer<String, User> producer;

    Consumer<String, Double> consumer;

    @BeforeAll
    public void init() {
        producer = kafkaTestUtils.getProducer(embeddedKafkaBroker);
        consumer = kafkaTestUtils.getDoubleConsumer(embeddedKafkaBroker, "consumer-test-group");

        consumer.subscribe(Collections.singleton(topologyConfig.usersOutput()));
    }

    @AfterAll
    public void close() {
        producer.flush();
        producer.close();

        consumer.close();
    }

    @Test
    public void shouldReturnAverageFollowersPerEachKey() {

        var key1 = "k1";
        var key2 = "k2";
        var data = List.of(
                Pair.create(key1, 24),
                Pair.create(key2, 30),
                Pair.create(key2, 25),
                Pair.create(key1, 89),
                Pair.create(key2, 15),
                Pair.create(key1, 12),
                Pair.create(key1, 32),
                Pair.create(key2, 5)

        );
        var expectedResultKey1 = data.stream().filter(v -> v.getLeft().equals(key1)).mapToInt(Pair::getRight).average().getAsDouble();

        var expectedResultKey2 = data.stream().filter(v -> v.getLeft().equals(key2)).mapToInt(Pair::getRight).average().getAsDouble();

        data.forEach(it -> sendUser(it.getLeft(), it.getRight()));

        var records = org.springframework.kafka.test.utils.KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(3)).records(topologyConfig.usersOutput());

        var iterator = records.iterator();
        var checkedRecords = 0;
        while (iterator.hasNext()) {
            checkedRecords++;
            var outputRecord = iterator.next();
            if (outputRecord.key().equals(key1)) {
                assertEquals(expectedResultKey1, outputRecord.value());
            }
            if (outputRecord.key().equals(key2)) {
                assertEquals(expectedResultKey2, outputRecord.value());
            }
        }
        assertEquals(2, checkedRecords);
    }

    private void sendUser(String key, int followers) {
        var user = User.newBuilder()
                .setId(ThreadLocalRandom.current().nextInt(5, 5000))
                .setName("name")
                .setFriendsCount(10)
                .setFollowerCount(followers)
                .build();

        var record = new ProducerRecord<>(topologyConfig.usersInput(), 1, System.currentTimeMillis(), key, user);
        producer.send(record);

    }
}