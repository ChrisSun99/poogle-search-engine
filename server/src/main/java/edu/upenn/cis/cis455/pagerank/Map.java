package edu.upenn.cis.cis455.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Map extends Mapper<Text, Text, Text, Text> {
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
		Node node = Node.fromString(value.toString());
		if (node.getAdjacentNodes() != null && node.getAdjacentNodes().length > 0) {
			// The pageRank value sent out by this node.
			double outValue = node.getValue() / (double)node.getAdjacentNodes().length;
			// Traverse all adjacent nodes and propagate pageRank to them.
			for (int i = 0; i < node.getAdjacentNodes().length; i++) {
				Node adjacentNode = new Node();
				adjacentNode.setValue(outValue);
				adjacentNode.setNode(false);
				context.write(new Text(node.getAdjacentNodes()[i]), new Text(adjacentNode.toString()));
			}
		}
	}
}
