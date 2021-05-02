package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.crawler.utils.Security;
import edu.upenn.cis.cis455.storage.AWSFactory;
import edu.upenn.cis.cis455.storage.AWSInstance;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DocumentFetcherBolt implements IRichBolt {

    static final Logger logger = LogManager.getLogger(DocumentFetcherBolt.class);

    /**
     * The `DocumentFetcherBolt` fetches the document for a url and returns it as a
     * String.
     */
    Fields schema = new Fields("domain", "url", "document", "contentType");

    /**
     * To make it easier to debug: we have a unique ID for each instance.
     */
    String executorId = UUID.randomUUID().toString();

    /**
     * This is where we send our output stream.
     */
    private OutputCollector collector;

    /**
     * Interface for database methods.
     */
    DatabaseEnv database;
    AWSInstance awsEnv;

    /**
     * Max document size.
     */
    int maxDocumentSize;

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    @Override
    public void prepare(Map<String, String> config, TopologyContext context, OutputCollector coll) {
        collector = coll;
        database = StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
        maxDocumentSize = Integer.parseInt(config.get(MAX_DOCUMENT_SIZE)) * MEGABYTE_BYTES;
        awsEnv = AWSFactory.getDatabaseInstance();

    }

    @Override
    public boolean execute(Tuple input) {
        String domain = input.getStringByField("domain");
        String url = input.getStringByField("url");
        logger.debug(getExecutorId() + " received " + url);
        logger.debug(url + ": received by document fetcher");

        // Validate url document with HEAD request according to content type.
        // If the url isn't valid, we drop it.
        logger.debug(url + ": validating document with head request");
        Map<String, String> responseHeaders = new HashMap<String, String>();

        if (!isDocumentValid(url, responseHeaders)) {
            logger.debug(url + ": document is invalid");
            logger.debug(url + " type: " + responseHeaders.get("Content-Type"));
            return true;
        }
        logger.debug(url + ": validated");

        logger.info(url + ": downloading");
        String content = HTTP.makeRequest(url, GET_REQUEST, maxDocumentSize, null);

        if (content == null) {
            logger.error(url + ": error fetching document");
            return true;
        }

        // Content-seen filter.
        String hash = Security.md5Hash(content);
        if (database.containsHashContent(hash)) {
            logger.info(url + ": content seen before");
            return true;
        }
        database.addContentSeen(hash);

        // Store document in database.
        logger.info(url + ": storing document in aws");
        String contentType = responseHeaders.get(CONTENT_TYPE_HEADER);
        database.addDocument(url, content, contentType);
        // awsEnv.putDocument(url, content);
        CrawlerState.count.incrementAndGet();

        logger.debug(getExecutorId() + " emitting content for " + url);
        collector.emit(new Values<Object>(domain, url, content, contentType), getExecutorId());
        return true;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void setRouter(StreamRouter router) {
        collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return schema;
    }

    ///////////////////////////////////////////////////
    // Helper Methods
    ///////////////////////////////////////////////////

    /**
     * Makes a HEAD request to check if document content type is valid.
     */
    private boolean isDocumentValid(String url, Map<String, String> responseHeaders) {

        String response = HTTP.makeRequest(url, HEAD_REQUEST, maxDocumentSize, responseHeaders);

        // Null response means that there was an error making the request.
        if (response == null) {
            return false;
        }

        // NOTE: Got rid of content length because a lot of sites don't send it on head
        // requests.
        String contentType = responseHeaders.get(CONTENT_TYPE_HEADER);

        if (contentType == null || !VALID_FILE_TYPES_SET.contains(contentType) && !contentType.endsWith("+xml")) {
            logger.debug(url + ": invalid file type");
            return false;
        }

        return true;
    }
}
