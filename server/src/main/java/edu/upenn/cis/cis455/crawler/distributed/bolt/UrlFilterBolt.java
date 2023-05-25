package edu.upenn.cis.cis455.crawler.distributed.bolt;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import crawlercommons.robots.SimpleRobotRules;
import edu.upenn.cis.cis455.crawler.utils.RobotRulesLRUCache;
import edu.upenn.cis.cis455.crawler.utils.UrlUtils;

public class UrlFilterBolt extends BaseBasicBolt {

    final BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100_000, 0.01);
    final RobotRulesLRUCache robotRulesLRUCache = new RobotRulesLRUCache();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String url = input.getStringByField("url");
        // avoids duplication
        if (!bloomFilter.mightContain(url)) {
            bloomFilter.put(url);
            // checks robots.txt
            SimpleRobotRules robotRules = robotRulesLRUCache.get(UrlUtils.robots(url));
            if (robotRules.isAllowed(url)) {
                collector.emit(new Values(url, robotRules.getCrawlDelay()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "crawlDelay"));
    }

}
