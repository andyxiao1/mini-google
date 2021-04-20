package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.CrawlerQueue;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
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
     * Domain based frontier queue for urls to be processed.
     */
    CrawlerQueue queue;

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
        queue = CrawlerQueue.getSingleton();
        database = (DatabaseEnv) StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        logger.info(getExecutorId() + " received " + url);

        if (database.containsUrl(url)) {
            logger.info(url + ": url seen before");
            return;
        }

        database.addUrl(url);
        queue.addUrl(url);
        logger.info(url + ": added to queue");
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void setRouter(IStreamRouter router) {
    }

    @Override
    public Fields getSchema() {
        return schema;
    }
}
