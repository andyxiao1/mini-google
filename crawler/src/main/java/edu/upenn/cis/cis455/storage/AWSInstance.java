package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.DynamoDBBackoffStrategy;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AWSInstance {

    static AmazonDynamoDB dynamoDB;
    private String tableName;
    private String bucketName;
    private int numDocs;
    static AmazonS3 s3;

    public AWSInstance() {

        /*
         * The ProfileCredentialsProvider will return your [default] credential profile
         * by reading from the credentials file located at
         * (/home/vagrant/.aws/credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (/home/vagrant/.aws/credentials), and is in valid format.", e);
        }

        s3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();

        bucketName = "cis455-finalproject";

        dynamoDB = AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1")
                .build();

        tableName = "docs";
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(
                        new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

        // Create table if it does not exist yet

        TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
        // wait for the table to move into ACTIVE state
        try {
            TableUtils.waitUntilActive(dynamoDB, tableName);
        } catch (TableNeverTransitionedToStateException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();

        numDocs = Math.toIntExact(tableDescription.getItemCount());
        // System.out.println("num docs: " + numDocs);

        // DescribeTableRequest describeTableRequest = new
        // DescribeTableRequest().withTableName(tableName);

        // TableDescription tableDescription =
        // dynamoDB.describeTable(describeTableRequest).getTable();

    }

    // public int putDocument(String url, String content) {

    // int id = url.hashCode();
    // Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

    // item.put("url", new AttributeValue(url));
    // item.put("id", new AttributeValue().withN(Integer.toString(id)));
    // item.put("content", new AttributeValue(content));

    // PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
    // PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
    // return id;
    // }

    public int putDocument(Document doc) {

        String url = doc.getUrl();
        String content = doc.getContent();

        int id = url.hashCode();
        String urlhash = "" + id;
        String contentHash = "" + content.hashCode();

        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

        item.put("url", new AttributeValue(url));
        item.put("id", new AttributeValue().withN(Integer.toString(id)));
        item.put("contentHash", new AttributeValue(contentHash));

        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

        createFileInS3(urlhash, content);

        return id;

    }

    public int putDocument(String url, String content) {

        int id = url.hashCode();
        String urlhash = "" + id;
        String contentHash = "" + content.hashCode();

        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

        item.put("url", new AttributeValue(url));
        item.put("id", new AttributeValue().withN(Integer.toString(id)));
        item.put("contentHash", new AttributeValue(contentHash));

        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

        createFileInS3(urlhash, content);

        return id;

    }

    // public Document getDocument(int id) {
    // Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    // item.put("id", new AttributeValue().withN(Integer.toString(id)));
    // Map<String, AttributeValue> map = dynamoDB.getItem(tableName,
    // item).getItem();

    // return new Document(map.get("url").getS());

    // }

    public void createFileInS3(String hashedUrl, String contents) {

        try {
            s3.putObject(new PutObjectRequest(bucketName, hashedUrl, createFile(hashedUrl, contents)));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file to
     * Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createFile(String filename, String content) throws IOException {
        File file = File.createTempFile("filename", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(content);

        writer.close();

        return file;
    }

    // for testing purposes
    public static void main(String[] args) throws Exception {
        AWSInstance instance = new AWSInstance();
        System.out.println(instance.putDocument(new Document("test2", "test2")));
        // System.out.println(instance.getDocument(1));
    }

}
