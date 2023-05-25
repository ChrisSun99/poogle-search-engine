package edu.upenn.cis.cis455.searchengine;

import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementResult;
import edu.upenn.cis.cis455.crawler.utils.Stemmer;
import edu.upenn.cis.cis455.indexer.IndexerMapper;
import edu.upenn.cis.cis455.storage.berkeleydb.CacheDBWrapper;
import edu.upenn.cis.cis455.storage.berkeleydb.CacheResultInfo;
import edu.upenn.cis.cis455.storage.berkeleydb.DBWrapper;
import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBInterface;
import edu.upenn.cis.cis455.storage.dynamodb.UrlData;
import spark.Request;
import spark.Response;
import spark.Route;


public class SearchResultRouter implements Route {
	private List<String> queryTerms;
	private DynamoDBInterface db;
	private CacheDBWrapper cacheDA = new CacheDBWrapper();
	private HashMap<String, Float> cosineSimilarity;
	private Heap metaScore;
	private Heap finalMetaScore;
	private Heap indexerScore;
	private Heap finalScore;
	private int count;
	private int maxResults = 50;
	
	JSONObject queryResult;
	JSONObject searchInformation;
	JSONArray items;
	String queryResultString;

	
	public SearchResultRouter() {
		queryTerms = new ArrayList<>();
		db = new DynamoDBInterface();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String handle(Request request, Response response) throws IOException  {
		try {
			DBWrapper.openDBWrapper(SearchServer.dbDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
		metaScore = new Heap(100);
		finalMetaScore = new Heap(100);
		finalScore = new Heap(100);
		count = 0;
		queryResult = new JSONObject();
		searchInformation = new JSONObject();
		items = new JSONArray();
		
		long start = System.currentTimeMillis();
		String query = request.queryParams("query");
		// TODO: OpenNLP for spell check
		// TODO: If query from cache, need to change searchInformation
		if (cacheDA.getCacheInfo(query) != null) {
			queryResultString = getCachedResults(query);
//			System.out.println("Get from cache: " + queryResultString);
		} else {
			queryTerms = getQueryTerms(query, false);
			if (queryTerms.size() == 0) {
				queryTerms = getQueryTerms(query, true);
			}
			cosineSimilarity = new HashMap<String, Float>();
			ExecutorService pool = Executors.newFixedThreadPool(4);
			Callable<HashMap<String, Float>> callableCosineSim = new CosineSimilarityCallable(queryTerms);
			
			//######################
			Callable<Heap> metaScoreCallable = new MetaCosineSimilarityCallable(queryTerms);
			Future<Heap> metaFuture = pool.submit(metaScoreCallable);
			//######################
			Future<HashMap<String, Float>> cosSimFuture = pool.submit(callableCosineSim);
			try {
				cosineSimilarity = cosSimFuture.get();
				//######################
				metaScore = metaFuture.get();
				//######################
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			
//			computeMetaScores();
//			if (finalMetaScore.size() < maxResults) {
//				System.out.println("Calculating content score");
//				computeContentScores();
//			}
			computeContentScores();
			if (finalScore.size() < maxResults) {
				computeMetaScores();
			}
			getRankedResults();
			pool.shutdown();
			long end = System.currentTimeMillis();
			searchInformation.put("formattedSearchTime", String.valueOf((end - start)/1000));
			searchInformation.put("formattedTotalResults", String.valueOf(maxResults));
			queryResult.put("searchInformation", searchInformation);
			queryResult.put("items", items);
			queryResultString = queryResult.toString();
			CacheResultInfo cacheInfo = new CacheResultInfo(query, queryResultString, new Date());
			cacheDA.addCacheInfo(cacheInfo);
			System.out.println("Saved to cache");
		}
		DBWrapper.closeDB();
		response.status(200);
//		response.type("text/html");
		response.type("application/json");
		System.out.println("result"+queryResultString);
		return queryResultString;
	}
	
	/**
	 * Compute content score based on indexer and pagerank weights 
	 */
	private void computeContentScores() {
		indexerScore = new Heap(100);
		ExecuteStatementRequest req;
		ExecuteStatementResult result;
		for (String md5 : cosineSimilarity.keySet()) {
			float score = (float) (0.17*cosineSimilarity.get(md5));
			indexerScore.add(md5, score);
		}
		for (int i = 0; i < 100 && !indexerScore.isEmpty(); i++) {
			SimpleEntry<String, Float> entry = indexerScore.remove();
			String md5 = entry.getKey();
			Float cosSim = entry.getValue();
			req = db.getUrlWithHighestPageRankScoreRequest(md5);
			result = db.getUrlWithHighestPageRankScoreResult(req);
			if (result == null || result.getItems().size() == 0) {
				continue;
			}
			UrlData urlData = db.marshallIntoUrlData(result);
			
			finalScore.add(urlData.getUrl(), (float)(0.6 * cosSim + 0.4 * urlData.getWeight()));
//			System.out.println("Content score: " + urlData.getUrl() + " " + cosSim + " " + urlData.getWeight());
		}
	}
	
	
	/**
	 * Compute metadata score based on indexer and pagerank weights 
	 */
	private void computeMetaScores() {
		finalMetaScore = new Heap(100);
		while (!metaScore.isEmpty()) {
			SimpleEntry<String, Float> entry = metaScore.remove();
			String md5 = entry.getKey();
			ExecuteStatementRequest req;
			ExecuteStatementResult result;
			req = db.getUrlWithHighestPageRankScoreRequest(md5);
			result = db.getUrlWithHighestPageRankScoreResult(req);
			if (result == null || result.getItems().size() == 0) {
				continue;
			}
			UrlData urlData = db.marshallIntoUrlData(result);
			
			if (cosineSimilarity.containsKey(md5)) {
				finalMetaScore.add(urlData.getUrl(), (float) (0.6*cosineSimilarity.get(md5) + 0.4*urlData.getWeight()));
//				System.out.println("meta score: "+urlData.getUrl() + " " + cosineSimilarity.get(md5) + " " + urlData.getWeight());
				cosineSimilarity.remove(md5);
			} else {
				finalMetaScore.add(urlData.getUrl(), (float) (0.6*entry.getValue() + 0.4*urlData.getWeight()));
//				System.out.println("meta score: "+urlData.getUrl() + " " + entry.getValue() + " " + urlData.getWeight());
			}
		}
	}
	
	/**
	 * Retrieve query results from cache
	 * @param query
	 * @return
	 */
	private String getCachedResults(String query) {
		String cachedResults = cacheDA.getCacheInfo(query).getResult();
		return cachedResults;
	}
	
	/**
	 * Get final rank based on metadata and content scores 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void getRankedResults() throws IOException {
		while (count < maxResults && !finalScore.isEmpty()) {
			try {
				SimpleEntry<String, Float> finalResults = finalScore.remove();
				
//				Document doc = Jsoup.connect.follow(finalResults.getKey()).get();
				Document doc = Jsoup.connect(finalResults.getKey())
						.followRedirects(true)
						.get();
			    JSONObject item = new JSONObject();
			    String snippet = null;
			    if (doc.body() == null || !doc.body().hasText()) {
			    	snippet = "Preview not available";
			    } else if (doc.body().text().length() < 100) {
			    	snippet = doc.body().text() + "...";
			    } else {
			    	snippet = doc.body().text().substring(0, 100)+"...";
			    }
				item.put("snippet", snippet);
				item.put("link", finalResults.getKey());
				item.put("displayLink", finalResults.getKey());
				item.put("title", doc.title());
				JSONObject pagemap = new JSONObject();
				JSONArray cse_image = new JSONArray();
				JSONObject inner_image = new JSONObject();
				Element img = doc.select("img[src$=.jpg]").first();
				if (img != null) {
					inner_image.put("src", doc.select("img[src$=.jpg]").first().absUrl("src"));
				} else {
					inner_image.put("src", "");
				}
				cse_image.add(inner_image);
				pagemap.put("cse_image", cse_image);
				item.put("pagemap", pagemap);
				items.add(item);
				count++;
			} catch (UnsupportedMimeTypeException e) {
				e.printStackTrace();
			} catch (HttpStatusException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		while (count < maxResults && !finalMetaScore.isEmpty()) {
			try {
				SimpleEntry<String, Float> finalResults = finalMetaScore.remove();
				Document doc = Jsoup.connect(finalResults.getKey()).get();
			    JSONObject item = new JSONObject();
			    String snippet = doc.body().text().substring(0, 100)+"...";
				item.put("snippet", snippet);
				item.put("link", finalResults.getKey());
				item.put("displayLink", finalResults.getKey());
				item.put("title", doc.title());
				JSONObject pagemap = new JSONObject();
				JSONArray cse_image = new JSONArray();
				JSONObject inner_image = new JSONObject();
				Element img = doc.select("img[src$=.jpg]").first();
				if (img != null) {
					inner_image.put("src", doc.select("img[src$=.jpg]").first().absUrl("src"));
				} else {
					inner_image.put("src", "");
				}
				cse_image.add(inner_image);
				pagemap.put("cse_image", cse_image);
				item.put("pagemap", pagemap);
				items.add(item);
				count++;
			} catch (UnsupportedMimeTypeException e) {
				e.printStackTrace();
			} catch (HttpStatusException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * Reorganize query terms 
	 * @param content
	 * @param includeStopWords
	 * @return
	 */
	private List<String> getQueryTerms(String content, Boolean includeStopWords) {
		List<String> queryTerms = new ArrayList<>();
		StringTokenizer tokenizer = new StringTokenizer(content, " ,.?\"!-");
		String word;
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
	    	word = word.trim().toLowerCase().replaceAll("[^a-z0-9 ]", "");
	    	if (includeStopWords || (!IndexerMapper.stopwords.contains(word) && !word.equals(""))){
	    		queryTerms.add(stem(word));
	    	}
		}
		return queryTerms;
	}
	
	private String stem(String word) {
		Stemmer stemmer = new Stemmer();
		char[] charArray = word.toCharArray();
		stemmer.add(charArray, word.length());
		stemmer.stem();
		return stemmer.toString();
	}
}
