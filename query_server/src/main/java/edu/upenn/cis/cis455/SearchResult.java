package edu.upenn.cis.cis455;

import com.amazonaws.services.dynamodbv2.document.Item;

public class SearchResult implements Comparable<SearchResult> {
	Item docItem;
	double itemScore;
	String docId;
	
	public SearchResult(Item documentItem, double itemScore) {
		this.docItem = documentItem;
		this.itemScore = itemScore;
	}
	
	public SearchResult(String docId, double itemScore) {
		this.docId = docId;
		this.itemScore = itemScore;
	}
	
	
	public Item getItem() {
		return this.docItem;
	}
	
	@Override
	public int compareTo(SearchResult o) {
		if (this.itemScore > o.itemScore) {
			return 1;
		} else if (this.itemScore < o.itemScore) {
			return -1;
		} else {
			return 0;
		}
	}

}
