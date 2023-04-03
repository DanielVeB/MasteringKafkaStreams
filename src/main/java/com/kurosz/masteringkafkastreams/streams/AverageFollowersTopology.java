package com.kurosz.masteringkafkastreams.streams;

import com.kurosz.masteringkafkastreams.avro.User;
import com.kurosz.masteringkafkastreams.config.KafkaTopologyConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class AverageFollowersTopology {

    private final SerdeUtils serdeUtils;

    private final KafkaTopologyConfig kafkaTopologyConfig;

    Logger logger = LoggerFactory.getLogger(MessagesTopology.class);


    public AverageFollowersTopology(SerdeUtils serdeUtils, KafkaTopologyConfig kafkaTopologyConfig) {
        this.serdeUtils = serdeUtils;
        this.kafkaTopologyConfig = kafkaTopologyConfig;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        Serde<User> userSerde = serdeUtils.getValueSerde();

        Serde<Average> averageSerde = new JsonSerde<>(Average.class);

        Serde<Double> doubleSerde = Serdes.Double();

        streamsBuilder.stream(kafkaTopologyConfig.usersInput(), Consumed.with(Serdes.String(), userSerde).withValueSerde(userSerde))
                .peek((k, v) -> logger.info("Key: {}. Followers: {}", k, v.getFollowerCount()))
                .groupByKey(Grouped.with(Serdes.String(), userSerde).withValueSerde(userSerde))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)))
                .aggregate(Average::new, (k, tr, ave) -> {
                        logger.info("Aggregate new event: {}",tr);
                        ave.setSum( ave.getSum() + tr.getFollowerCount());
                        ave.setNum(ave.getNum() + 1);
                        return ave;
                    },Materialized.with(Serdes.String(), averageSerde)
                )
                .toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v.getSum() * 1.0 / v.getNum()))
                .peek((k, v) -> logger.info("Counted, key: {}, value: {}", k, v))
                .to(kafkaTopologyConfig.usersOutput(), Produced.with(Serdes.String(), doubleSerde).withValueSerde(doubleSerde));
    }

    private TimeWindows makeFixedTimeWindow() {
        return TimeWindows.ofSizeAndGrace(Duration.ofMillis(5000), Duration.ofMillis(5000)).advanceBy(Duration.ofMillis(5000));
    }


}
