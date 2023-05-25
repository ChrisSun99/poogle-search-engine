package edu.upenn.cis.cis455.crawler.distributed.bolt;

import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Tuple;

public class DbBatchBolt extends BaseBatchBolt<Object> {

    @Override
    public void prepare(Map<String,Object> conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void finishBatch() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }
    
}
