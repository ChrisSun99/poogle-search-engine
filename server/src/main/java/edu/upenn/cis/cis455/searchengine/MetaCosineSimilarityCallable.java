package edu.upenn.cis.cis455.searchengine;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBInterface;
import edu.upenn.cis.cis455.storage.dynamodb.InvertedIndexData;

public class MetaCosineSimilarityCallable implements Callable<Heap> {
	
	private List<String> query;
	
	private Heap metaScores;
	
	private Heap metaScoresTemp;
	
	private Map<String, Integer> urlWord;
	
	private DynamoDBInterface db = new DynamoDBInterface();
	
	public MetaCosineSimilarityCallable(List<String> query) {
		this.query = query;
	}
	
	@Override 
	public Heap call() throws Exception {
		// TODO Auto-generated method stub
		metaScores = new Heap(100);
		metaScoresTemp = new Heap(100);
		urlWord = new HashMap<>();
		int numQuery = query.size();
		for (String term : query) {
			List<InvertedIndexData> invidxDataList = db.getInvertedIndexData(term);
			if (invidxDataList == null) {
				System.out.println("No meta and title matches found for : " + term);
				continue;
			}
			
			if (invidxDataList.size() > 100)
				invidxDataList = invidxDataList.subList(0, 100);
			computeUrlWordMatches(invidxDataList);
		}
		
		while (!metaScoresTemp.isEmpty()) {
			SimpleEntry<String, Float> tempEntry = metaScoresTemp.remove();
			if (urlWord.get(tempEntry.getKey()) == numQuery)
				metaScores.add(tempEntry);
		}
		return metaScores;
	}
	
	private void computeUrlWordMatches(List<InvertedIndexData> postings) {		
		for (InvertedIndexData posting: postings) {
			String url = posting.getMd5();
			Float score = posting.getTfidf();
			metaScoresTemp.add(url, score);
			
			if (urlWord.containsKey(url)) {
				int count = urlWord.get(url);
				urlWord.put(url, count + 1);
			} else {
				urlWord.put(url, 1);
			}
		}
	}

}
