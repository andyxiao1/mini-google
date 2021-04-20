package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.crawler.utils.Security;
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
    Fields schema = new Fields("url", "document", "contentType", "isCachedVersion");

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
        database = (DatabaseEnv) StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
        maxDocumentSize = Integer.parseInt(config.get(MAX_DOCUMENT_SIZE)) * MEGABYTE_BYTES;
    }

    @Override
    public boolean execute(Tuple input) {
        String url = input.getStringByField("url");
        logger.info(getExecutorId() + " received " + url);

        if (url == null) {
            collector.emit(new Values<Object>(null, null, null, null), getExecutorId());
            return true;
        }

        // Validate url document with HEAD request according to content type and length.
        // If the url isn't valid, we drop it.
        logger.info(url + ": validating document with head request");
        Map<String, String> responseHeaders = new HashMap<String, String>();

        if (!isDocumentValid(url, responseHeaders)) {
            logger.info(url + ": document is invalid");
            return true;
        }
        logger.info(url + ": validated");

        // Check if we can use cached version.
        long lastModified = convertDateToEpoch(responseHeaders.get(LAST_MODIFIED_HEADER));
        long lastFetched = database.getDocumentLastFetch(url);
        boolean shouldUseCachedVersion = lastModified != -1 && lastFetched != -1 && lastModified < lastFetched;

        String content = null;

        if (shouldUseCachedVersion) {
            logger.info(url + ": using cached version");
            content = database.getDocument(url);
        } else {
            content = HTTP.makeRequest(url, GET_REQUEST, maxDocumentSize, null);
            logger.info(url + ": downloading");

            if (content == null) {
                logger.info(url + ": error fetching document");
                return true;
            }
        }

        // Content-seen filter.
        String hash = Security.md5Hash(content);
        if (database.containsHashContent(hash)) {
            logger.info(url + ": content seen before");
            return true;
        }
        database.addContentSeen(hash);

        logger.info(getExecutorId() + " emitting content for " + url);
        String contentType = responseHeaders.get(CONTENT_TYPE_HEADER);
        collector.emit(new Values<Object>(url, content, contentType, shouldUseCachedVersion), getExecutorId());
        return true;
    }

    @Override
    public void cleanup() {
        if (database != null) {
            database.close();
        }
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
     * Makes a HEAD request to check if document content type and length are valid.
     */
    private boolean isDocumentValid(String url, Map<String, String> responseHeaders) {

        String response = HTTP.makeRequest(url, HEAD_REQUEST, maxDocumentSize, responseHeaders);

        // Null response means that there was an error making the request.
        if (response == null) {
            return false;
        }

        int contentLength = -1;
        try {
            contentLength = Integer.parseInt(responseHeaders.get(CONTENT_LENGTH_HEADER));
        } catch (NumberFormatException e) {
            logger.error("Error validating document: Content-Length is not a valid number");
            logger.error(e);
            return false;
        }
        String contentType = responseHeaders.get(CONTENT_TYPE_HEADER);

        if (contentLength > maxDocumentSize) {
            logger.info(url + ": file too big");
            return false;
        }

        if (contentType == null || !VALID_FILE_TYPES_SET.contains(contentType) && !contentType.endsWith("+xml")) {
            logger.info(url + ": invalid file type");
            return false;
        }

        return true;
    }

    /**
     * Converts a HTTP date to epoch time.
     */
    private long convertDateToEpoch(String date) {
        if (date == null) {
            return -1;
        }
        Instant dateInstant = ZonedDateTime.parse(date, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        return dateInstant.toEpochMilli();
    }
}
