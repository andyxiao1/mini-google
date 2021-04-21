package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.worker.CrawlerQueue;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.RobotsInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UrlSpout implements IRichSpout {

    static final Logger logger = LogManager.getLogger(UrlSpout.class);

    /**
     * The `UrlSpout` retrieves a url from the queue and emits it as a String.
     */
    Fields schema = new Fields("domain", "url");

    /**
     * To make it easier to debug: we have a unique ID for each instance.
     */
    String executorId = UUID.randomUUID().toString();

    /**
     * The collector is the destination for tuples; you "emit" tuples there.
     */
    SpoutOutputCollector collector;

    /**
     * Domain based frontier queue for urls to be processed.
     */
    CrawlerQueue queue;

    /**
     * Interface for database methods.
     */
    DatabaseEnv database;

    int maxCount;

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    @Override
    public void open(Map<String, String> config, TopologyContext context, SpoutOutputCollector coll) {
        collector = coll;
        queue = CrawlerQueue.getSingleton();
        database = (DatabaseEnv) StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
        maxCount = Integer.parseInt(config.get(CRAWL_COUNT));
    }

    @Override
    public void close() {
        if (database != null) {
            database.close();
        }
    }

    @Override
    public boolean nextTuple() {

        if (queue.isEmpty() || CrawlerState.isShutdown) {
            return true;
        }

        String domain = queue.getNextDomain();

        // Check if the domain has robots info.
        // Note: We end early and don't update the queue order at all, so the same
        // domain/url will be at the head.
        if (!queue.hasRobotsInfo(domain)) {
            logger.debug(domain + ": retrieving associated robots.txt information");
            fetchRobotsInfo(domain);
            return true;
        }

        RobotsInfo robotsInfo = queue.getRobotsInfo(domain);
        long lastAccessedTime = queue.getLastAccessedTime(domain);

        // Check that enough time has passed with respect to the `Crawl-delay` since the
        // last request to this domain. If not, skip this domain.
        if (!hasCrawlDelayPassed(lastAccessedTime, robotsInfo)) {
            queue.skipDomain();
            return true;
        }

        String url = queue.removeUrl();

        // Check that url is not disallowed. If it is disallowed, drop it.
        if (!isUrlAllowed(url, robotsInfo)) {
            logger.debug(url + ": not allowed by robots.txt");
            return true;
        }

        // Note: We are tracking access times by when the url gets emitted, so no two
        // urls for a domain will be emitted within the crawl-delay, there may be some
        // conditions where this doesn't work.
        queue.accessDomain(domain);

        logger.debug(getExecutorId() + " emitting " + url);
        collector.emit(new Values<Object>(domain, url), getExecutorId());
        return true;
    }

    @Override
    public void setRouter(StreamRouter router) {
        collector.setRouter(router);
    }

    ///////////////////////////////////////////////////
    // Helper Methods
    ///////////////////////////////////////////////////

    /**
     * First check to see if robots info exists in database, if not we fetch it with
     * an HTTP request. Update the queue domain properties appropriately.
     */
    private void fetchRobotsInfo(String domain) {
        // Note: We should always be able to fetch the robots.txt without any worry of
        // the crawl-delay because it should be the first request we make to any domain.

        RobotsInfo robotsInfo = null;
        if (database.containsRobotsInfo(domain)) {
            logger.debug(domain + ": fetching robots.txt from database");
            robotsInfo = database.getRobotsInfo(domain);
        } else {
            logger.debug(domain + ": making http request for robots.txt");
            String robotsUrl = domain + ROBOTS_PATH;
            String robotsFile = HTTP.makeRequest(robotsUrl, GET_REQUEST, MAX_ROBOTS_FILE_SIZE, null);

            if (CrawlerState.isShutdown) {
                return;
            }

            robotsInfo = database.addRobotsInfo(domain, robotsFile);
            queue.accessDomain(domain);
        }

        queue.setRobotsInfo(domain, robotsInfo);
    }

    /**
     * Check to see if enough time has passed to make a new request to the domain.
     */
    private boolean hasCrawlDelayPassed(long lastAccessedTime, RobotsInfo robotsInfo) {
        if (lastAccessedTime == -1) {
            return true;
        }

        long currTime = Instant.now().toEpochMilli();
        long currDelay = (currTime - lastAccessedTime) / 1000;
        return currDelay > robotsInfo.crawlDelay;
    }

    /**
     * Check if url is in robots.txt's disallowed paths.
     */
    private boolean isUrlAllowed(String url, RobotsInfo robotsInfo) {
        String filePath = (new URLInfo(url)).getFilePath();
        for (String disallowedPath : robotsInfo.disallowedPaths) {
            if (filePath.startsWith(disallowedPath)) {
                return false;
            }
        }
        return true;
    }
}
