package edu.upenn.cis.cis455;

import spark.Request;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import spark.Response;
import spark.Route;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.sleepycat.json_simple.JsonArray;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;

public class SearchHandler implements Route {
	
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
		
		System.out.println(item.get("rank"));
		
		BigDecimal pageScore = (BigDecimal) item.get("rank");
		
		return pageScore.doubleValue();
	}
	
	public Item getDocumentItem(String doc_id) {
		Item item = docTable.getItem("id", doc_id);
		
		return item;
	}
	
	/**
	 * 
	 * @param term the term to query for
	 * @return
	 */
	public String query(String term) {
		StringBuilder outcomes = new StringBuilder();
		
		// lets do a map from term -> c of that lemma
		List<String> terms = preprocessWord(term);
		
		Map<String, ArrayList<Item>> docIdToItem = new HashMap<String, ArrayList<Item>>(); 
		
		// query items multiplied by idf
		Map<String, Double> queryWeightMap = new HashMap<String, Double>();
		
		for (int i = 0; i < terms.size(); i++) {
			String t = terms.get(i);
			
			Index index = indexDb.getIndex("term-tfidf-index");
			QuerySpec spec = new QuerySpec()
			    .withKeyConditionExpression("term = :v_term")
			    .withValueMap(new ValueMap()
			        .withString(":v_term", t))
			    .withScanIndexForward(false).withMaxResultSize(this.maxToQueryLimit);
			
			logger.debug("Searching for " + term);
			
			ItemCollection<QueryOutcome> items = index.query(spec);
			
			Iterator<Item> iter = items.iterator(); 
			int count = 0;
			
			while (iter.hasNext()) {
				Item item = iter.next();
				
				// Checking if the query term already is in the query
				if (count == 0) {
					// Building Query Weight Map 
					if (queryWeightMap.containsKey(t)) {
						// add the idf to this
						double weight = queryWeightMap.get(t);
						// like multiplying the actual query string by each words idf
						queryWeightMap.put(t , weight + weight);
					} else {
						
						queryWeightMap.put(t , item.getDouble("idf"));
					}
				}
				
//				String output = iter.next().toJSONPretty();
				String doc_id = (String) item.get("doc_id");
				String jsonRep = item.toJSON();
				
				if (i == 0) {
					ArrayList<Item> arrList = new ArrayList<Item>();
					arrList.add(item);
					docIdToItem.put(doc_id, arrList);
					// TODO: i think this is right
				} else if (docIdToItem.containsKey(doc_id)) {
					// only if its already in there do i put this in there
					docIdToItem.get(doc_id).add(item);
				}
				count++;
			}
		}
		
		String output = computeRanking(docIdToItem, queryWeightMap);
		
		
		
		
		return prettyMap(docIdToItem);
	}
	
	private String computeRanking(Map<String, ArrayList<Item>> docIdToItem, Map<String, Double> queryWeightMap) {
		// Final intersection of docIds 
		Set docIdsSet = docIdToItem.keySet();
		
		
		// make a json array to send
		JsonArray returnJson = new JsonArray();
		
		// go through every docId, and get the ranking
		for (String docId : docIdToItem.keySet()) {
			
			ArrayList<Item> docToTerms = docIdToItem.get(docId);
			
			double itemScore = 0;
			
			// query weight dot product
			for (Item queryItem : docToTerms) {
				String queryTerm = (String) queryItem.get("term");
				double qWeight = queryWeightMap.get(queryTerm);
				itemScore += queryItem.getDouble("tfidf") * qWeight;
			}
			
			// add pageRank
			double pgScore = getPageRankScore(docId);
			itemScore += pgScore;
			
			// add points if the terms show up in the title
			Item documentItem = getDocumentItem(docId);
			String title = documentItem.getString("title");
	        StanfordLemmatizer slem = new StanfordLemmatizer();
	        List<String> titleLemmas = slem.getLemmasList(title);
	        
	        int titleWeight = 10;
	        
	        
	        for (String queryTerm : queryWeightMap.keySet()) {
	        	if (titleLemmas.contains(queryTerm)) {
	        		itemScore += titleWeight;
	        	}
	        }
	        
	        
			
		}
		
		return null;
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
		String query = request.queryParams("query");
		
		logger.info("Sent query " + query);
		
        try {
        	// Step 1 get list of lemmas
        	List<String> terms = preprocessWord(query);
        	
        	// query for each of them 
        	StringBuilder sb = new StringBuilder();
        	for (String lemma : terms) {
        		String output = query(lemma);
        		sb.append(output);
        	}
        	
        	
            return sb.toString();
        }
        catch (Exception e) {
            System.err.println("Unable to read item: " + query);
//            System.err.println(spec);
            System.err.println(e.getMessage());
            return e.getMessage();
        }
	}
	
	public String prettyMap(Map<String, ArrayList<Item>> docIdToItem) {
		StringBuilder sb = new StringBuilder();
		
		for (String key : docIdToItem.keySet()) {
			sb.append("Doc_id: " + key + "\n");
			for (Item itm : docIdToItem.get(key)) {
				sb.append(itm.toJSONPretty() + "\n");
			}
		}
		
		return sb.toString();
	}
}
