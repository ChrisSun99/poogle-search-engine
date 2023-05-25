package edu.upenn.cis.cis455.pagerank;

import java.io.IOException;
import java.util.Arrays;

public class Node {
	// Page Rank value for the node
	private double value = 0.1;
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	
	// Adjacent Nodes for the node
	private String[] adjacentNodes;
	public String[] getAdjacentNodes() {
		return adjacentNodes;
	}
	public void setAdjacentNodes(String[] adjacentNodes) {
		this.adjacentNodes = adjacentNodes;
	}
	
	// Whether the node represents the node itself or the outvalue
	private boolean isNode = true;
	public boolean isNode() {
		return isNode;
	}
	public void setNode(boolean isNode) {
		this.isNode = isNode;
	}
	
	// Whether the adjacentNodes are empty
	public boolean isEmpty() {
		return adjacentNodes == null;
	}
	
	@Override
	public String toString() {
		String results = String.valueOf(value);
		if (adjacentNodes != null) {
			for (int i = 0; i < adjacentNodes.length; i++) {
				results += "\t" + adjacentNodes[i];
			}
		}
		results += "\t" + String.valueOf(isNode);
		return results;
	}
	
	static public Node fromString(String st) throws IOException {
		String[] parts = st.split("\t");
		if (parts.length < 2) {
			throw new IOException(
	          "Expected 2 or more parts but received " + parts.length);
	    }
		Node node = new Node();
		node.setValue(Double.valueOf(parts[0]));
		if (parts.length > 2) {
			node.setAdjacentNodes(Arrays.copyOfRange(parts, 1, parts.length - 1));
		}
		node.setNode(Boolean.valueOf(parts[parts.length - 1]));
		return node;
	}
}

