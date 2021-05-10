package edu.upenn.cis.cis455;

import spark.Request;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import spark.Response;
import spark.Route;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.MinMaxPriorityQueue;

import com.sleepycat.json_simple.JsonArray;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;


public class SearchHandler implements Route {
	
	class DocIdToIdf {
		String doc_id;
		double idf;
		String term;
		
		public DocIdToIdf(String doc_id, double idf, String term) {
			this.doc_id = doc_id;
			this.idf = idf;
			this.term = term;
		}
		
		public String getDocId() {
			return this.doc_id;
		}
		
		public double getIdf() {
			return this.idf;
		}
		
		public String getTerm() {
			return this.term;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Doc_ID: " + this.doc_id).append("\n");
			sb.append("idf: " + this.idf).append("\n");
			sb.append("term: " + this.term).append("\n");
			return sb.toString();
		}
		
	}
	
	private static final Logger logger = LogManager.getLogger(SearchHandler.class);
	
	private Table indexDb;
	
	private Table pageRankTable;
	
	private Table docTable;
	
	private int maxToQueryLimit;
	
	private int maxToShowLimit = 10;
	
	public SearchHandler(Table indexDb, int maxQueryLimit, Table pageRankTable, Table docTable) {
		this.indexDb = indexDb;
		this.maxToQueryLimit = maxQueryLimit;
		this.pageRankTable = pageRankTable;
		this.docTable = docTable;
	}
	
	public double getPageRankScore(String doc_id) {
		Item item = pageRankTable.getItem("doc_id", doc_id);
		
		if (item == null) {
			return 0;
		}
		
		BigDecimal pageScore = (BigDecimal) item.get("rank");
		
		return pageScore.doubleValue();
	}
	
	public Item getDocumentItem(String doc_id) {
		Item item = docTable.getItem("id", doc_id);
		
		return item;
	}

	
	private String prettyQueryMap(Map<String, Double> queryWeightMap) {
		StringBuilder sb = new StringBuilder();
		
		for (String key : queryWeightMap.keySet()) {
			sb.append("Key: " + key + " Item: " + queryWeightMap.get(key) + "\n");
		}
		
		return sb.toString();
	}

	private String computeRanking(Map<String, ArrayList<DocIdToIdf>> docIdToItem, Map<String, Double> queryWeightMap) {
		
        // Creating empty priority queue
//		PriorityQueue<SearchResult> q = PriorityQueue.maximumSize(maxToShowLimit).create();
		PriorityQueue<SearchResult> q = new PriorityQueue<SearchResult>();
		
		// make a json array to send
		JsonArray returnJson = new JsonArray();
		logger.info(docIdToItem.keySet().size());
		// go through every docId, and get the ranking
		for (String docId : docIdToItem.keySet()) {
			ArrayList<DocIdToIdf> docToTerms = docIdToItem.get(docId);
			double itemScore = 0;
			
			// query weight dot product
			for (DocIdToIdf queryItem : docToTerms) {
				String queryTerm = queryItem.getTerm();
				double qWeight = queryWeightMap.get(queryTerm);
				itemScore += queryItem.getIdf() * qWeight;
			}
			
			// add pageRank
			double pgScore = getPageRankScore(docId);
			
			itemScore += pgScore;
			
			// add points if the terms show up in the title
//			Item documentItem = getDocumentItem(docId);
//			if (pgScore > 10) {
//				logger.info("GOOD PAGE RANK " + documentItem.getString("url"));
//			}
//			
//			String title = documentItem.getString("title");
//			
//			logger.debug(title);
			
			// FOR CHECKING TITLE, to imrove 
//			if (title != null && !title.isBlank()) {
//		        StanfordLemmatizer slem = new StanfordLemmatizer();
//		        List<String> titleLemmas = slem.getLemmasList(title);
//		        
//		        int titleWeight = 10;
//		        
//		        for (String queryTerm : queryWeightMap.keySet()) {
//		        	if (titleLemmas.contains(queryTerm)) {
//		        		itemScore += titleWeight;
//		        	}
//		        }
//			}
			
			q.add(new SearchResult(docId, itemScore));
			if (q.size() >= maxToShowLimit) {
				q.poll();
			}
		}
		
		List<String> toReverse = new ArrayList<String>();
		
		for (int i = 0 ; i < maxToShowLimit; i++) {
			
			SearchResult sresult = q.poll();
			
			if (sresult == null) {
				break;
			}
			
			logger.info("SCORE: " + sresult.itemScore);
			
			Item documentItem = getDocumentItem(sresult.docId);
			
			logger.info(documentItem.toJSONPretty());
			
			toReverse.add(documentItem.toJSON());
			
//			returnJson.add(it.toJSON());
		}
		
		Collections.reverse(toReverse);
		
		returnJson.addAll(toReverse);
		
		return returnJson.toJson();
	}
	
	public List<String> preprocessWord(String term) {
		// lemmatization
        StanfordLemmatizer slem = new StanfordLemmatizer();
        List<String> terms = slem.getLemmasList(term);
        
		System.out.println(terms);
		
		for (String t : terms) {
			t = t.toLowerCase();
		}
		
		System.out.println(terms);
		
		return terms;
	}
	
	@Override
	public Object handle(Request request, Response response) throws Exception {
		String query = request.queryParams("q");
		
		logger.info("Sent query " + query);
		try {
			String queryAnswer = queryNewSchema(query);
			return queryAnswer;
		} catch (Exception e) {
			JsonArray returnJson = new JsonArray();
			return returnJson.toJson();
		}
	}
	
	public String prettyMap(Map<String, ArrayList<DocIdToIdf>> docIdToItem) {
		StringBuilder sb = new StringBuilder();
		
		for (String key : docIdToItem.keySet()) {
			sb.append("Doc_id: " + key + "\n");
			for (DocIdToIdf itm : docIdToItem.get(key)) {
				sb.append(itm.toString() + "\n");
			}
		}
		
		return sb.toString();
	}
	
	
	/**
	 * 
	 * @param term the term to query for
	 * @return
	 */
	public String queryNewSchema(String term) {
		// lets do a map from term -> c of that lemma
		List<String> terms = preprocessWord(term);
		
		Map<String, ArrayList<DocIdToIdf>> docIdToItem = new HashMap<String, ArrayList<DocIdToIdf>>(); 
		
		// query items multiplied by idf
		Map<String, Double> queryWeightMap = new HashMap<String, Double>();
		
		int rankSearch = 1;
		
		// outer loop is for every term in the query
		for (int i = 0; i < terms.size(); i++) {
			String t = terms.get(i);
			
			// TODO: fix this ranking? 
			GetItemSpec spec = new GetItemSpec().withPrimaryKey("word", t, "rank", rankSearch);
			
			Item indexItem = indexDb.getItem(spec);
			
			if (indexItem == null) {
				logger.error("No matching term");
				// make a json array to send
				JsonArray returnJson = new JsonArray();
				
				return returnJson.toJson();
			}
			
			logger.debug("Searching for " + t);
			
			int count = 0;
			
			if (queryWeightMap.containsKey(t)) {
				// add the idf to this
				double weight = queryWeightMap.get(t);
				// like multiplying the actual query string by each words idf
				queryWeightMap.put(t , weight + weight);
				logger.debug("putting " + t + " into query weight map with contains");
			} else {
				logger.debug("putting " + t + " into query weight map not contains with weight " + indexItem.getDouble("idf"));
				queryWeightMap.put(t , indexItem.getDouble("idf"));
			}
			
			List<Map<String, Object>> docIdsList = indexItem.getList("docList");
			
			for (Map<String, Object> o : docIdsList) {
				logger.debug(o.toString());
				String doc_id = (String) o.get("docid");
				BigDecimal tfBig = (BigDecimal) o.get("tfIdf");
				double tfIdf = tfBig.doubleValue();
				logger.debug("TFIDF: " + tfIdf);
				
				// for every document for this term, create a new item here
				DocIdToIdf insert = new DocIdToIdf(doc_id, tfIdf, t);
				
				if (i == 0) {
					ArrayList<DocIdToIdf> arrList = new ArrayList<DocIdToIdf>();
					arrList.add(insert);
					docIdToItem.put(doc_id, arrList);
					logger.debug("putting first doc item into map" + arrList.toString() + " into docItems");
				} else if (docIdToItem.containsKey(doc_id)) {
					// only if its already in there do i put this in there, this does the union
					docIdToItem.get(doc_id).add(insert);
				}
			}
		}
		
		logger.debug("Query map: " + prettyQueryMap(queryWeightMap));
		
		logger.debug("Doc ID map: " + prettyMap(docIdToItem));
		
		logger.info("Done here");
		
		String output = computeRanking(docIdToItem, queryWeightMap);
		
		return output;
	}
	
}



