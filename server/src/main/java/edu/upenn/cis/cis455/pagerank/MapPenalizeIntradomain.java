package edu.upenn.cis.cis455.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MapPenalizeIntradomain extends Mapper<Text, Text, Text, Text> {
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
		Node node = Node.fromString(value.toString());
		URLInfo urlNode = new URLInfo(key.toString());
		String hostNameNode = urlNode.getHostName();
		if (node.getAdjacentNodes() != null && node.getAdjacentNodes().length > 0) {
			// The pageRank value sent out by this node.
			double outValue = node.getValue() / (double)node.getAdjacentNodes().length;
			// Calculate the number of interdomain links and intradomain links outbounded by this node
			int interdomainLinks = 0;
			int intradomainLinks = 0;
			for (int i = 0; i < node.getAdjacentNodes().length; i++) {
				URLInfo urlNeighbor = new URLInfo(node.getAdjacentNodes()[i]);
				String hostNameNeighbor = urlNeighbor.getHostName();
				if (hostNameNode.equals(hostNameNeighbor)) {
					intradomainLinks++;
				} else {
					interdomainLinks++;
				}
			}
			// Traverse all adjacent nodes and propagate pageRank to them.
			for (int i = 0; i < node.getAdjacentNodes().length; i++) {
				Node adjacentNode = new Node();
				URLInfo urlNeighbor = new URLInfo(node.getAdjacentNodes()[i]);
				String hostNameNeighbor = urlNeighbor.getHostName();
				if (hostNameNode.equals(hostNameNeighbor)) {
					adjacentNode.setValue(outValue / 2);
				} else {
					adjacentNode.setValue(outValue * ((double)node.getAdjacentNodes().length - ((double)intradomainLinks) / 2) / (double)interdomainLinks);
				}
				adjacentNode.setNode(false);
				context.write(new Text(node.getAdjacentNodes()[i]), new Text(adjacentNode.toString()));
			}
		}
	}
}