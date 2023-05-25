package edu.upenn.cis.cis455.searchengine;

import java.util.*;
import java.util.concurrent.*;

import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBInterface;
import edu.upenn.cis.cis455.storage.dynamodb.InvertedIndexData;

/**
 * Cosine similarity callable
 *
 */
public class CosineSimilarityCallable implements Callable<HashMap<String, Float>> {
	private List<String> query;
	private HashMap<String, Float> cosineSimilarity;
	private HashMap<String, Float> queryTf;
	private HashMap<String, Float> seenUrlsDenominator;
	private float queryDenominator;
	private DynamoDBInterface db = new DynamoDBInterface();
	
	public CosineSimilarityCallable(List<String> query) {
		this.query = query;
	}
	
	@Override
	public HashMap<String, Float> call() throws Exception {
		queryDenominator = 0;
		List<InvertedIndexData> invidxDataList;
		cosineSimilarity = new HashMap<String, Float>();
		seenUrlsDenominator = new HashMap<String, Float>();
		if (!query.isEmpty()) {
			computeQueryTf();	
			for (String term : query) {
				invidxDataList = db.getInvertedIndexData(term);
				if (invidxDataList != null) {
					if (invidxDataList.size() > 500)
						invidxDataList = invidxDataList.subList(0, 500);
					computeCosineSimilarity(term, invidxDataList);
				}	
			}			
			for (String md5: cosineSimilarity.keySet()) {
				float tfidf = cosineSimilarity.get(md5);
				float denominator = seenUrlsDenominator.get(md5);
				float cosineSim = (float) (tfidf/(Math.sqrt(denominator + queryDenominator)));
				cosineSimilarity.put(md5, cosineSim);
			}
		}	
		return cosineSimilarity;
	}
	
	/**
	 * Compute the term frequency of query
	 */
	private void computeQueryTf() {
		HashMap<String, Float> tfMap = new HashMap<String, Float>();
		queryTf = new HashMap<String, Float>();
		for (String term: query) {
			if (tfMap.containsKey(term)) {
				float tf = tfMap.get(term);
				tfMap.put(term, tf + 1);
			} else {
				tfMap.put(term, (float) 1);
			}
		}
		
		for (String term: tfMap.keySet()) {
			queryTf.put(term, tfMap.get(term));
		}
	}
	
	/**
	 * Compute cosine similarity between the query and words in DB
	 * @param term
	 * @param invidxList
	 */
	private void computeCosineSimilarity(String term, List<InvertedIndexData> invidxList){
		String md5;
		float invidxTfidf;
		float idf = invidxList.get(0).getIdf();
		float queryTermTfidf = (float) (queryTf.get(term) * idf);
		queryDenominator += Math.pow(queryTermTfidf, 2);
		
		for (InvertedIndexData invidx: invidxList) {
			md5 = invidx.getMd5();
			if (cosineSimilarity.containsKey(md5)) {
				float currTfidf = cosineSimilarity.get(md5);
				invidxTfidf = invidx.getTfidf();
				cosineSimilarity.put(md5, currTfidf + invidxTfidf*queryTermTfidf);
				float currDenominator = seenUrlsDenominator.get(md5);
				seenUrlsDenominator.put(md5, currDenominator + invidxTfidf*invidxTfidf);
			} else {
				invidxTfidf = invidx.getTfidf();
				cosineSimilarity.put(md5, invidxTfidf*queryTermTfidf);
				seenUrlsDenominator.put(md5, invidxTfidf*invidxTfidf);
			}
		}
	}
}
