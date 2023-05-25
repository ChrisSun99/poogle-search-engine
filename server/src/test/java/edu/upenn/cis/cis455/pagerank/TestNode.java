package edu.upenn.cis.cis455.pagerank;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.logging.log4j.Level;

public class TestNode {
    @Before
    public void setUp() {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);
    }
    
    @Test
    public void testNode() throws IOException {
    	Node node = new Node();
    	node.setNode(true);
    	String[] str = new String[3];
    	str[0] = "1"; str[1] = "2"; str[2] = "3";
    	node.setAdjacentNodes(str);
    	node.setValue(1);
    	String nodeInString = node.toString();
    	assertTrue(nodeInString.equals("1.0\t1\t2\t3\ttrue"));
    	Node newNode = Node.fromString(nodeInString);
    	assertTrue(node.getValue() == newNode.getValue());
    	assertTrue(Arrays.equals(node.getAdjacentNodes(), newNode.getAdjacentNodes()));
    	assertTrue(node.isNode() == newNode.isNode());
    }

    
    @After
    public void tearDown() {}
}
