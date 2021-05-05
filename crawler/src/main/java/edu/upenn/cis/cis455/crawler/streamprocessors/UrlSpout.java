package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.net.MalformedURLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
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
     * Interface for database methods.
     */
    DatabaseEnv database;

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
        database = StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
    }

    @Override
    public void close() {
    }

    @Override
    public boolean nextTuple() {

        String domain = database.crawlQueueRemoveLeft();
        if (domain == null || CrawlerState.isShutdown.get()) {
            return true;
        }

        boolean shouldFetchRobots = !database.containsRobotsInfo(domain);

        // Since we have removed the domain, only 1 thread can fetch robots.txt.
        if (shouldFetchRobots) {
            logger.debug(domain + ": fetching robots.txt start");
            fetchRobotsInfo(domain);
            logger.debug(domain + ": fetching robots.txt end");
            database.crawlQueueAddLeft(domain);
            return true;
        }

        RobotsInfo robotsInfo = database.getRobotsInfo(domain);

        // Check that enough time has passed with respect to the `Crawl-delay`.
        if (!hasCrawlDelayPassed(robotsInfo)) {
            database.crawlQueueAddRight(domain);
            logger.debug(domain + "crawl delay: " + robotsInfo.crawlDelay);
            return true;
        }

        String url = database.removeUrl(domain);
        logger.debug(url + ": removed from queue");

        // Check that url is not disallowed. If it is disallowed, drop it.
        if (!isUrlAllowed(url, robotsInfo)) {
            database.crawlQueueAddRight(domain);
            logger.debug(url + ": not allowed by robots.txt");
            return true;
        }

        if (!isValidUrl(url)) {
            logger.debug(url + ": doesn't pass url filter");
            return true;
        }

        // Note: We are tracking access times by when the url gets emitted, so no two
        // urls for a domain will be emitted within the crawl-delay, there may be some
        // conditions where this doesn't work.
        database.accessDomain(robotsInfo);

        logger.debug(getExecutorId() + " emitting " + url);
        collector.emit(new Values<Object>(domain, url), getExecutorId());
        database.crawlQueueAddRight(domain);
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

        logger.debug(domain + ": making http request for robots.txt");
        String robotsUrl = domain + ROBOTS_PATH;
        String robotsFile = HTTP.makeRequest(robotsUrl, GET_REQUEST, MAX_ROBOTS_FILE_SIZE, null);

        RobotsInfo robotsInfo = database.addRobotsInfo(domain, robotsFile);
        database.accessDomain(robotsInfo);
    }

    /**
     * Check to see if enough time has passed to make a new request to the domain.
     */
    private boolean hasCrawlDelayPassed(RobotsInfo robotsInfo) {
        if (robotsInfo.crawlDelay == 0) {
            return true;
        }

        long currTime = Instant.now().toEpochMilli();
        long currDelay = (currTime - robotsInfo.lastAccessedTime) / 1000;
        return currDelay >= robotsInfo.crawlDelay;
    }

    /**
     * Check if url is in robots.txt's disallowed paths.
     */
    private boolean isUrlAllowed(String url, RobotsInfo robotsInfo) {
        String filePath = null;
        try {
            filePath = (new URLInfo(url)).getFilePath();
        } catch (MalformedURLException e) {
            return false;
        }
        for (String disallowedPath : robotsInfo.disallowedPaths) {
            if (filePath.startsWith(disallowedPath)) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidUrl(String link) {
        if (link.contains("wikipedia") && !link.contains("en.wikipedia")) {
            return false;
        }

        return true;
    }
}
