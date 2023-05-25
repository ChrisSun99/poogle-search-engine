package edu.upenn.cis.cis455.crawler.distributed.bolt;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;

public class DocFetchBolt extends BaseBasicBolt {

    final int DEFAULT_SIZE_LIMIT = 1024 * 1024; // 1MB
    final int MAX_CRAWL_DELAY = 60 * 1000; // 1 minute

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String url = input.getStringByField("url");
        // Crawl Delay
        try {
            int crawlDelay = Integer.parseInt((String)input.getStringByField("crawlDelay"));
            if (crawlDelay < 0) crawlDelay = 0;
            if (crawlDelay > MAX_CRAWL_DELAY) crawlDelay = MAX_CRAWL_DELAY;
            Thread.sleep(crawlDelay);
        } catch (Exception e) {
            // do nothing
        }
        // Fetch Document
        try {
            Response response = Jsoup.connect(url)
                    .method(Method.GET)
                    .userAgent("cis455crawler")
                    .ignoreContentType(true)
                    .followRedirects(true)
                    .maxBodySize(DEFAULT_SIZE_LIMIT)
                    .execute();
            collector.emit(new Values(url, response));
        } catch (MalformedURLException e) {
            // should never happen
        } catch (HttpStatusException e) {
            // TODO: need to remove url doc from db if it exists
            // TODO: 429 Too Many Requests, add back to queue with delay
            // collector.emit(streamId, tuple)
        } catch (IOException e) {
            // TODO: how to handle???
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "response"));
    }
    
}