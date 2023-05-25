package edu.upenn.cis.cis455.storage.dynamodb;

import java.io.Serializable;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName=DynamoDBInterface.INVERTED_INDEX_TABLE)
public class InvertedIndexData implements Serializable {

	private static final long serialVersionUID = 1L;
	private String word; 
    private String md5; 
    private Integer count;
    private Float tf;
    private Float idf;
    private Float tfidf;
    
    @DynamoDBHashKey(attributeName="word")
    public String getWord() {
        return word;
    }
    
    public void setWord(String word) {
        this.word = word;
    }
    
    @DynamoDBRangeKey(attributeName="md5")
    public String getMd5() {
        return md5;
    }
    
    public void setMd5(String md5) {
        this.md5 = md5;
    }
    
    @DynamoDBAttribute(attributeName="count")
    public Integer getCount() {
        return count;
    }
    
    public void setCount(Integer count) {
        this.count = count;
    }
    
    @DynamoDBAttribute(attributeName="tf")
    public Float getTf() {
        return tf;
    }
    
    public void setTf(Float tf) {
        this.tf = tf;
    }

    @DynamoDBAttribute(attributeName="idf")
    public Float getIdf() {
        return idf;
    }
    
    public void setIdf(Float idf) {
        this.idf = idf;
    }

    @DynamoDBAttribute(attributeName="tfidf")
    public Float getTfidf() {
        return tfidf;
    }
    
    public void setTfidf(Float tfidf) {
        this.tfidf = tfidf;
    }
}
