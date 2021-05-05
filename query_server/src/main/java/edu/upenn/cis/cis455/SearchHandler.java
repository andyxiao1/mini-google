package edu.upenn.cis.cis455;

import spark.Request;

import spark.Response;
import spark.Route;

import java.util.Arrays;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.document.Item;

public class SearchHandler implements Route {
	
	private Table indexDb;
	
	public SearchHandler(Table indexDb) {
		this.indexDb = indexDb;
	}

	@Override
	public Object handle(Request request, Response response) throws Exception {
		String query = request.queryParams("query");

        GetItemSpec spec = new GetItemSpec().withPrimaryKey("term", query);
        
        try {
            System.out.println("Attempting to read the item...");
            Item outcome = indexDb.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);
            return outcome.get("doc_id");
        }
        catch (Exception e) {
            System.err.println("Unable to read item: " + query);
            System.err.println(spec);
            System.err.println(e.getMessage());
            return e.getMessage();
        }
	}
}
