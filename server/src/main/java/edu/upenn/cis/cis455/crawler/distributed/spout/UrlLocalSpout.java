package edu.upenn.cis.cis455.crawler.distributed.spout;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class UrlLocalSpout extends BaseRichSpout {

    BlockingQueue<String> urlQueue;

    SpoutOutputCollector _collector;

    @Override
    @SuppressWarnings("unchecked")
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        urlQueue = (BlockingQueue<String>) conf.get("urlQueue");
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            String url = urlQueue.take();
            if (!url.isEmpty()) {
                // log.debug(getExecutorId() + " emitting " + url);
                System.out.println("Emitting " + url);
                _collector.emit(new Values(url));
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    }
    
}
