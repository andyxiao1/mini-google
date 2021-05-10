package edu.upenn.cis.cis455;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import edu.stanford.nlp.ie.machinereading.structure.Span;
import edu.stanford.nlp.simple.Sentence;

public class TestLemmas {
	
	@Test
	public void testLemmaSize() {
		StanfordLemmatizer s = new StanfordLemmatizer();
		
		String output = s.getLemma("Hello's");
		assertEquals(output, "hello");
//		s.getLemmasList("Hello's");
		
	}
	
	@Test
	public void testPageRankGet() {
        
        String tableName = "inverted_index_final";
        
        int maxLimitReturn = 1000;
        
        
    	String pageRankTableName = "PageRank_final_20_iterations";
        
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (/home/.aws/credentials), and is in valid format.", e);
        }
        
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1).withCredentials(credentialsProvider).build();
        
        DynamoDB dynamoDB = new DynamoDB(client);
        
        Table table = dynamoDB.getTable(tableName);
        
        Table pageRankTable = dynamoDB.getTable(pageRankTableName);
		
        Table docTable = dynamoDB.getTable("documents-final");
		
		SearchHandler s = new SearchHandler(table, maxLimitReturn, pageRankTable, docTable);
		
		double ans = s.getPageRankScore("02d1add998e02a74054a23c103a9deb5");
		
		System.out.println(ans);
		
		assertTrue(0.5029727989599819 == ans);
	}
	
	@Test
	public void testPageRankGetNoItem() {
        
        String tableName = "inverted_index_final";
        
        int maxLimitReturn = 1000;
        
    	String pageRankTableName = "PageRank_final_20_iterations";
        
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (/home/.aws/credentials), and is in valid format.", e);
        }
        
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1).withCredentials(credentialsProvider).build();
        
        DynamoDB dynamoDB = new DynamoDB(client);
        
        Table table = dynamoDB.getTable(tableName);
        
        Table pageRankTable = dynamoDB.getTable(pageRankTableName);
		
        Table docTable = dynamoDB.getTable("documents-final");
		
		SearchHandler s = new SearchHandler(table, maxLimitReturn, pageRankTable, docTable);
		
		double ans = s.getPageRankScore("not_here");
		
		assertTrue(0 == ans);
		
		
	}
	
//	@Test
//	public void testStanfordPos() {
//       Sentence sent = new Sentence("Who is the best king in town? Obama? Grant?");
////       int index = sent.algorithms().headOfSpan(new Span(0, sent.length()));  // Should return 1
////       System.out.println(sent.lemma(index));
//       System.out.println(sent.nerTags());
//       
//       Sentence sent2 = new Sentence("What does the ostrich eat for breakfast?");
//       System.out.println(sent2.nerTags());
//       
//       // shows NER doesn't really help i dont think for the waste of time it may caus
//	}
//	
	
	
	
	
	

}
