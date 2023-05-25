package edu.upenn.cis.cis455.storage.berkeleydb;

import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/*
 * Search query cache with BerkeleyDB
 */

@Entity 
public class CacheResultInfo {
	
	/*
	 * Search query
	 */
	@PrimaryKey
	private String query; 
	
	/*
	 * previously formed JSON String
	 */
	private String result; 
	
	/*
	 * Last accessed time of the query item
	 */
	private Date lastAccessTime;
	
	public CacheResultInfo() {}
	
	public CacheResultInfo(String query, String result, Date lastAccessTime) {
		this.query = query; 
		this.result = result; 
		this.lastAccessTime = lastAccessTime;
	}
	
	public String getQuery() {
		return this.query;
	}
	
	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getResult() {
		return this.result;
	}

	public void setResult(String result) {
		this.result = result;
	}
	
	public Date getLastAccessTime() {
		return this.lastAccessTime;
	}
	
	public void setLastAccessTime(Date lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}
}
