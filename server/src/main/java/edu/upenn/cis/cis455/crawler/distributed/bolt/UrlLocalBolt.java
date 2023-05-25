package edu.upenn.cis.cis455.crawler.distributed.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Tuple;

public class UrlLocalBolt extends BaseBatchBolt<Object> {

    BlockingQueue<String> urlQueue;

    List<String> urlLocalQueue = new ArrayList<>();

    BatchOutputCollector _collector;

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        urlQueue = (BlockingQueue<String>) conf.get("urlQueue");
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        urlLocalQueue.add(url);
    }

    @Override
    public void finishBatch() {
        urlQueue.addAll(urlLocalQueue);
        urlLocalQueue = new ArrayList<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
