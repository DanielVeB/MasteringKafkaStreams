package com.kurosz.masteringkafkastreams.streams;

import com.kurosz.masteringkafkastreams.avro.Tweet;
import com.kurosz.masteringkafkastreams.avro.TweetLang;
import com.kurosz.masteringkafkastreams.config.KafkaTopologyConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class MessagesTopology {

    private final SerdeUtils serdeUtils;

    private final KafkaTopologyConfig kafkaTopologyConfig;

    Logger logger = LoggerFactory.getLogger(MessagesTopology.class);


    public MessagesTopology(SerdeUtils serdeUtils, KafkaTopologyConfig kafkaTopologyConfig) {
        this.serdeUtils = serdeUtils;
        this.kafkaTopologyConfig = kafkaTopologyConfig;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        Serde<Tweet> tweetSerde = serdeUtils.getValueSerde();

        var branches = streamsBuilder.stream(kafkaTopologyConfig.tweetsInput(), Consumed.with(Serdes.String(), tweetSerde))
                .peek((k, v) -> logger.info("Processed new tweet: {}", v.getText()))
                .split(Named.as("lang-"))
                .branch((k, v) -> TweetLang.EN.equals(v.getLang()),
                        Branched.withFunction(ks -> ks.peek((k, v) -> logger.info("English tweet")), "en"))
                .branch((k, v) -> !TweetLang.EN.equals(v.getLang()),
                        Branched.withFunction(ks -> ks.peek((k, v) -> logger.info("Non english tweet")), "non-en"))
                .noDefaultBranch();

        branches.get("lang-en").merge(branches.get("lang-non-en"))
                .to(kafkaTopologyConfig.tweetsOutput(), Produced.with(Serdes.String(), tweetSerde));
    }
}
