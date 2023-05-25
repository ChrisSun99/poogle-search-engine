package edu.upenn.cis.cis455.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static final double DAMPING_FACTOR = 0.85;
	public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
	public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
	public static enum Counter { CONV_DELTAS }
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Node originalNode = new Node();
		double sum = 0;
		
		for (Text text: values) {
			Node node = Node.fromString(text.toString());
			if (node.isNode()) {
				originalNode = node;
			} else {
				sum += node.getValue();
			}
		}
		
		double newValue = DAMPING_FACTOR * sum + (1.0 - DAMPING_FACTOR);
		
		double delta = originalNode.getValue() - newValue;
		
		originalNode.setValue(newValue);
		
		context.write(key, new Text(originalNode.toString()));
		long scaledDelta = Math.abs((long) (delta * CONVERGENCE_SCALING_FACTOR));
		context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);
	}
}

