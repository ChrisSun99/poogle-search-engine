package edu.upenn.cis.cis455.indexer;

import edu.upenn.cis.cis455.indexer.InvIdx;

/**
 * Class for indexer output
 */
public class InvIdx implements Comparable<InvIdx> {
	private String docId; 
	private float TFIDF;
	private float IDF;
	
	public InvIdx(String docId, float TFIDF, float IDF) {
		this.docId = docId; 
		this.TFIDF = TFIDF;
		this.IDF = IDF; 
	}
	
	public String getDocId() {
		return this.docId;
	}
	
	public float getIDF() {
		return this.IDF;
	}
	
	public float getTFIDF() {
		return this.TFIDF;
	}

	@Override
	public int compareTo(InvIdx compareTFTDF) {
		// TODO Auto-generated method stub
		int other = (int) ((InvIdx)compareTFTDF).getTFIDF();
		return other - (int)this.TFIDF;
	}
}

