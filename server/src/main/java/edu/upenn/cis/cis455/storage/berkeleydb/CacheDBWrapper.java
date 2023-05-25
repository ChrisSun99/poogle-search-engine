package edu.upenn.cis.cis455.storage.berkeleydb;

import com.sleepycat.persist.PrimaryIndex;

/**
 * BerkeleyDB cache connection
 */
public class CacheDBWrapper {
	/**
	 * Add query and json string to cache
	 * @param cache
	 */
	public void addCacheInfo(CacheResultInfo cache) {
		if (DBWrapper.getStore() != null) {
			PrimaryIndex<String, CacheResultInfo> results = DBWrapper.getStore().getPrimaryIndex(String.class, CacheResultInfo.class);
			if (results != null) {
				results.put(cache);
			}
		}
	}
	
	/**
	 * Get the json string of corresponding query from cache
	 * @param word
	 * @return cacheInfo
	 */
	public CacheResultInfo getCacheInfo(String word) {
		CacheResultInfo cacheInfo = null;
		if (DBWrapper.getStore() != null) {
			PrimaryIndex<String, CacheResultInfo> resultsByQuery = DBWrapper.getStore().getPrimaryIndex(String.class, CacheResultInfo.class);
			if (resultsByQuery != null) {	
				cacheInfo = resultsByQuery.get(word);
			}
		}
		return cacheInfo;
	}
}
