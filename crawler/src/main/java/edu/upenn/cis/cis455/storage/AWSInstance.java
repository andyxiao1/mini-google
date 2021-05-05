package edu.upenn.cis.cis455.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.amazonaws.services.dynamodbv2.document.Table;
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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.utils.Security;

public class AWSInstance {
    static final Logger logger = LogManager.getLogger(AWSInstance.class);

    static AmazonDynamoDB dynamoDB;
    private String tableName;
    private String bucketName;
    private final int max_doc_size = 100000000;
    String currentS3Document = "";
    private Map<String, Set<String>> urlToDests = new HashMap<String, Set<String>>();
    private String docname;
    private String awsDocumentsFolder;
    private String awsUrlMapFolder;

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

        bucketName = "555finalproject";
        // filePath = "url_mappings.csv";
        dynamoDB = AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1")
                .build();
        docname = UUID.randomUUID().toString();

        // Set defaults.
        tableName = "documents";
        awsDocumentsFolder = "documents";
        awsUrlMapFolder = "urlmap";
    }

    public void setConfig(String documentsFolder, String urlMapFolder, String documentsTableName) {
        tableName = documentsTableName;
        awsDocumentsFolder = documentsFolder;
        awsUrlMapFolder = urlMapFolder;
    }

    public synchronized String getDocument(String url) {
        String id = Security.md5Hash("https://en.wikipedia.org/wiki/Main_Page");
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

        item.put("id", new AttributeValue(id));

        Map<String, AttributeValue> map = dynamoDB.getItem("documents", item).getItem();

        String docname = map.get("docname").getS();
        int endIdx = Integer.parseInt(map.get("endIdx").getN());
        int startIdx = Integer.parseInt(map.get("startIdx").getN());
        System.out.println(docname);
        System.out.println(endIdx);

        S3Object object = s3.getObject(new GetObjectRequest("555finalproject", docname));

        byte[] content;
        try {
            content = object.getObjectContent().readAllBytes();
            return (new String(content, "UTF-8").substring(startIdx, endIdx));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "ERROR";
    }

    public synchronized String putDocument(String url, String content, int numLinks, String requestTime, String domain,
            String docExcerpt, String title) {

        String id = Security.md5Hash(url);
        String contentHash = "" + content.hashCode();

        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

        int startIdx = currentS3Document.length();
        currentS3Document += "*startid" + id + "*\n" + content + "\n*endid*\n";
        int endIdx = currentS3Document.length();

        item.put("url", new AttributeValue(url));
        item.put("id", new AttributeValue(id));
        item.put("contentHash", new AttributeValue(contentHash));
        item.put("docname", new AttributeValue(docname));
        item.put("startIdx", new AttributeValue().withN(Integer.toString(startIdx)));
        item.put("endIdx", new AttributeValue().withN(Integer.toString(endIdx)));
        item.put("length", new AttributeValue().withN(Integer.toString(endIdx - startIdx)));
        item.put("numLinks", new AttributeValue().withN(Integer.toString(numLinks)));
        item.put("requestTime", new AttributeValue().withN(requestTime));
        item.put("domain", new AttributeValue(domain));
        item.put("docExcerpt", new AttributeValue(docExcerpt));
        item.put("title", new AttributeValue(title));

        // System.out.println("num byteS: " + currentS3Document.getBytes().length);
        if (currentS3Document.getBytes().length > max_doc_size) {
            createFileInS3(docname, currentS3Document);
            currentS3Document = "";
            resetDocname();
        }

        if (urlToDests.size() > 10000) {
            sendURLStoS3();
            urlToDests = new HashMap<String, Set<String>>();
        }

        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        dynamoDB.putItem(putItemRequest);

        return id;

    }

    private void resetDocname() {
        docname = UUID.randomUUID().toString();
    }

    public void createFileInS3(String hashedUrl, String contents) {

        try {
            s3.putObject(new PutObjectRequest(bucketName + "/" + awsDocumentsFolder, hashedUrl + ".txt",
                    createFile(hashedUrl, contents, ".txt")));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        sendURLStoS3();
        createFileInS3(docname, currentS3Document);
        logger.info("AWS closed.");
    }

    private void sendURLStoS3() {
        File f = createCSV(docname);
        s3.putObject(new PutObjectRequest(bucketName + "/" + awsUrlMapFolder, docname + ".csv", f));

    }

    public File createCSV(String fn) {

        FileWriter csvWriter;
        String name = fn + ".csv";
        File f = new File(name);
        try {

            f.createNewFile();
            f.deleteOnExit();

            csvWriter = new FileWriter(f);

            csvWriter.append("src");
            csvWriter.append(",");
            csvWriter.append("dst");
            csvWriter.append("\n");

            // add data to csv
            for (Map.Entry<String, Set<String>> urlPair : urlToDests.entrySet()) {

                for (String s : urlPair.getValue()) {

                    csvWriter.append(String.join(",", urlPair.getKey(), s));
                    csvWriter.append("\n");
                }

            }

            csvWriter.flush();
            csvWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return f;

    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file to
     * Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createFile(String filename, String content, String suffix) throws IOException {
        File file = File.createTempFile(filename, suffix);
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(content);

        writer.close();

        return file;
    }

    // for testing purposes
    public static void main(String[] args) throws Exception {
        /*
         * AWSInstance instance = new AWSInstance();
         *
         *
         * FileWriter csvWriter; String name = "adam"+".csv"; File f = new File(name);
         * try {
         *
         * f.createNewFile(); //f.setReadable(true); // f.setWritable(true);
         * //f.deleteOnExit();
         *
         * csvWriter = new FileWriter(f); // CSVWriter writer = new
         * CSVWriter(csvWriter);
         *
         * String csvContent = ""; csvContent += "src,dst\n"; csvWriter.append("src");
         * csvWriter.append(","); csvWriter.append("dst"); csvWriter.append("\n");
         *
         *
         * csvContent += "src,dst\n";
         *
         * //add data to csv HashMap<String,Set<String>> urlToDest = new
         * HashMap<String,Set<String>>(); urlToDest.put("test1", new HashSet<String>());
         * urlToDest.get("test1").add("test2"); for (Map.Entry<String, Set<String>>
         * urlPair : urlToDest.entrySet()) {
         *
         * for (String s : urlPair.getValue()) {
         *
         * csvContent += String.join(",", urlPair.getKey(), s) + "\n";
         *
         * csvWriter.append(String.join(",", urlPair.getKey(), s));
         * csvWriter.append("\n"); }
         *
         * }
         *
         * csvWriter.flush(); csvWriter.close(); s3.putObject(new
         * PutObjectRequest("555finalproject" + "/urlmap", "test2.csv", f));
         *
         *
         * } catch (IOException e) { e.printStackTrace(); }
         *
         *
         *
         */
        /*
         * AWSInstance instance = new AWSInstance();
         *
         *
         * String id = Security.md5Hash("https://en.wikipedia.org/wiki/Main_Page");
         * Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
         *
         * item.put("id", new AttributeValue(id));
         *
         * Map<String, AttributeValue> map =
         * dynamoDB.getItem("documents",item).getItem();
         *
         * String docname = map.get("docname").getS(); int endIdx =
         * Integer.parseInt(map.get("endIdx").getN()); int startIdx =
         * Integer.parseInt(map.get("startIdx").getN()); System.out.println(docname);
         * System.out.println(endIdx);
         *
         * S3Object object = s3.getObject(new GetObjectRequest("555finalproject",
         * docname));
         *
         *
         * byte[] content = object.getObjectContent().readAllBytes();
         * System.out.println(new String(content, "UTF-8").substring(startIdx,endIdx));
         */
    }

    public synchronized void addUrl(String url, String domain) {
        // System.out.println("url sent: " + docIndex++);
        if (!urlToDests.containsKey(Security.md5Hash(url))) {
            urlToDests.put(Security.md5Hash(url), new HashSet<String>());
        }
        urlToDests.get(Security.md5Hash(url)).add(Security.md5Hash(domain));

    }

}
