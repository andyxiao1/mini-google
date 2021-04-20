package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.CrawlerQueue;
import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.RobotsInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
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
    Fields schema = new Fields("url");

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
        maxCount = Integer.parseInt(config.get("COUNT"));
    }

    @Override
    public void close() {
    }

    @Override
    public void nextTuple() {

        if (queue.isEmpty() || CrawlerState.count >= maxCount) {
            collector.emit(new Values<Object>((Object) null));
            return;
        }

        CrawlerState.noMoreLinks = false;

        String domain = queue.getNextDomain();

        // Check if the domain has robots info.
        // Note: We end early and don't update the queue order at all, so the same
        // domain/url will be at the head.
        if (!queue.hasRobotsInfo(domain)) {
            logger.info(domain + ": retrieving associated robots.txt information");
            fetchRobotsInfo(domain);
            return;
        }

        RobotsInfo robotsInfo = queue.getRobotsInfo(domain);
        long lastAccessedTime = queue.getLastAccessedTime(domain);

        // Check that enough time has passed with respect to the `Crawl-delay` since the
        // last request to this domain. If not, skip this domain.
        if (!hasCrawlDelayPassed(lastAccessedTime, robotsInfo)) {
            queue.skipDomain();
            return;
        }

        String url = queue.removeUrl();

        // Check that url is not disallowed. If it is disallowed, drop it.
        if (!isUrlAllowed(url, robotsInfo)) {
            logger.info(url + ": not allowed by robots.txt");
            return;
        }

        // Note: We are tracking access times by when the url gets emitted, so no two
        // urls for a domain will be emitted within the crawl-delay, there may be some
        // conditions where this doesn't work.
        queue.accessDomain(domain);

        logger.info(getExecutorId() + " emitting " + url);
        CrawlerState.count++;
        collector.emit(new Values<Object>(url));
    }

    @Override
    public void setRouter(IStreamRouter router) {
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
            logger.info(domain + ": fetcihng robots.txt from database");
            robotsInfo = database.getRobotsInfo(domain);
        } else {
            logger.info(domain + ": making http request for robots.txt");
            String robotsUrl = domain + ROBOTS_PATH;
            String robotsFile = HTTP.makeRequest(robotsUrl, GET_REQUEST, MAX_ROBOTS_FILE_SIZE, null);
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
