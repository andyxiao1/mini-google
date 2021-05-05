package edu.upenn.cis.cis455;
import static spark.Spark.*;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

import spark.Spark;


public class WebServer {
	
	private static final Logger logger = LogManager.getLogger(WebServer.class);
	
    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);
        
        int port = Integer.parseInt(args[0]);
        
        Spark.port(port);
        
        
        String tableName = args[1];
        
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
        
        System.out.println("Starting server on port: " + port);
        
        
        
        get("/", (request, response) -> "hello world");
        
        get("/hello", (request, response) -> "world");
        
        get("/search", new SearchHandler(table));
        
        get("/shutdown", (request, response) -> {
        	stop();
        	return "";
        });
        
    }
}
