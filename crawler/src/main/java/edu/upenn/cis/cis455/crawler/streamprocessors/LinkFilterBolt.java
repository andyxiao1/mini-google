package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LinkFilterBolt implements IRichBolt {

    static final Logger logger = LogManager.getLogger(LinkFilterBolt.class);

    /**
     * The `LinkFilterBolt` doesn't stream anything.
     */
    Fields schema = new Fields();

    /**
     * To make it easier to debug: we have a unique ID for each instance.
     */
    String executorId = UUID.randomUUID().toString();

    /**
     * Interface for database methods.
     */
    DatabaseEnv database;

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map<String, String> config, TopologyContext context, OutputCollector coll) {
        database = StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
    }

    @Override
    public boolean execute(Tuple input) {
        String url = input.getStringByField("url");
        logger.debug(getExecutorId() + " received " + url);
        logger.debug(url + ": filtering");

        if (url == null || url.equals("")) {
            return true;
        }

        if (database.containsUrl(url)) {
            logger.debug(url + ": url seen before");
            return true;
        }

        if (!isValidUrl(url)) {
            logger.debug(url + ": doesn't pass url filter");
            return true;
        }

        database.addUrlSeen(url);
        database.addUrlToCrawlQueue(url);
        logger.debug(url + ": added to queue");
        return true;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void setRouter(StreamRouter router) {
    }

    @Override
    public Fields getSchema() {
        return schema;
    }

    private boolean isValidUrl(String link) {
        if (link.contains("wiki") && !link.contains("en.wikipedia")) {
            return false;
        }
        if (link.contains("wordpress") || link.contains("stackexchange") || link.contains("tumblr")) {
            return false;
        }

        return true;
    }
}
