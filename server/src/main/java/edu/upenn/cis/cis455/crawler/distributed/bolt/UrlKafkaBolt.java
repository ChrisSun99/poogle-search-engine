package edu.upenn.cis.cis455.crawler.distributed.bolt;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

public class UrlKafkaBolt extends KafkaBolt<String, String> {

    public UrlKafkaBolt() {
        super();

        Properties props = new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            // put(ProducerConfig.ACKS_CONFIG, "1");
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }};

        this.withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("url"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "url"));
    }

}
