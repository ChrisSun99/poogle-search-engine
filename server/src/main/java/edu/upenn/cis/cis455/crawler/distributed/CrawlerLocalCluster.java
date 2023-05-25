package edu.upenn.cis.cis455.crawler.distributed;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import edu.upenn.cis.cis455.crawler.distributed.bolt.UrlFilterBolt;
import edu.upenn.cis.cis455.crawler.distributed.bolt.UrlKafkaBolt;
import edu.upenn.cis.cis455.crawler.distributed.spout.UrlKafkaSpout;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.slf4j.event.Level;

public class CrawlerLocalCluster {
    // private static Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    public static void main(String[] args) throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setLevel("org.apache.storm", 
                org.apache.logging.log4j.Level.ERROR);

        TopologyBuilder builder = new TopologyBuilder();

        // builder.setSpout("kafkaSpout", new UrlKafkaSpout(), 2);
        // builder.setSpout("kafkaSpout", new TestSpout(), 1);
        // builder.setBolt("middle", new UrlFilterBolt(), 2).shuffleGrouping("kafkaSpout");
        // builder.setBolt("forwardToKafka", new UrlKafkaBolt(), 2).shuffleGrouping("middle");

        Config conf = new Config();
        // StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());
        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology("mytopology", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.shutdown();
        }

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                // local cluster took too long to exit
                System.exit(1);
            }
        }, 10000);
    }

}
