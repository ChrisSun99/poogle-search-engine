package edu.upenn.cis.cis455.indexer.meta;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.upenn.cis.cis455.indexer.InvIdx;

public class IndexerReducer extends Reducer<Text, Text, Text, Text> {
	private static Map<String, Integer> tf = null;
	private int df; 
	private final static int bucketSize = 119866;
	private final static String OUTPUT_PATH = "reducerOutput.txt";
	
	
	@Override 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		df = computeDF(values);
		List<InvIdx> invidices = sortInvidx();
    	String filename= OUTPUT_PATH;
        FileWriter fw = new FileWriter(filename,true);
    	for (InvIdx invidx : invidices) {
    		try {
	            fw.write(key.toString() + "\t");
	            fw.write(invidx.getDocId() + "\t");
	            fw.write(invidx.getIDF() + "\t");
	            fw.write(invidx.getTFIDF() + "\t");
	            fw.write("\n");
	            fw.flush();
    	        System.out.println(key.toString() + " " + invidx.getDocId() + " " + invidx.getIDF() + " " + invidx.getTFIDF());
	        } catch (IOException e) {
	      	  e.printStackTrace();
	        }
    	}
    	fw.close();
		
	}
	
	/**
     * Compute the term frequency
     * @param md5
     * @return
     */
    private int computeDF(Iterable<Text> md5) {
    	Set<String> docIdSet = new HashSet<>();
    	tf = new HashMap<>();
    	for (Text id: md5) {
    		String docId = id.toString();
    		docIdSet.add(docId);
    		if (tf.containsKey(docId)) {
    			int count = tf.get(docId);
    			tf.put(docId, count+1);
    		} else {
    			tf.put(docId, 1);
    		}
    	}
    	return docIdSet.size();
    }
    
    /**
     * Sort the idf and tfidf
     * @return
     */
    private List<InvIdx> sortInvidx() {
    	List<InvIdx> invidxList = new ArrayList<>();
		for (String docID: tf.keySet()) {
			float idf = (float) Math.log(bucketSize /df);
			float tfidf = tf.get(docID) * idf ;
			InvIdx newInvidx = new InvIdx(docID, tfidf, idf);
			invidxList.add(newInvidx);
		}
		Collections.sort(invidxList);
		return invidxList; 
    }
}
