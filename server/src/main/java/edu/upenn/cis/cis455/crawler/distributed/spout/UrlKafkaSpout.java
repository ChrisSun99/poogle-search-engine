package edu.upenn.cis.cis455.crawler.distributed.spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;


// Reference: https://github.com/apache/storm/blob/master/examples/storm-kafka-client-examples/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutTopologyMainNamedTopics.java
public class UrlKafkaSpout extends KafkaSpout<String, String> {

    String[] initialURLs = new String[] {
            "cis455/crawler/crawler-distributed-1",
            "cis455/crawler/crawler-distributed-2",
            "cis455/crawler/crawler-distributed-3",
            "cis455/crawler/crawler-distributed-4",
            "cis455/crawler/crawler-distributed-5",
        };
    
    public UrlKafkaSpout() {
        super(KafkaSpoutConfig.builder("localhost:9092", "url")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "url-group")
                .setRecordTranslator((r) -> new Values(r.value()), new Fields("url"))
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                // .setPollTimeoutMs(200)
                // .setOffsetCommitPeriodMs(30000)
                // .setMaxUncommittedOffsets(10000000)
                .build());
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        for (String url : initialURLs)
            collector.emit(new Values(url));
        super.open(conf, context, collector);
    }

}
