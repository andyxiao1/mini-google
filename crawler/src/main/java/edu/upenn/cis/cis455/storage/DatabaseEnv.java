package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;

public class DatabaseEnv {

    static final Logger logger = LogManager.getLogger(DatabaseEnv.class);

    Environment env;
    EntityStore store;
    PrimaryIndex<Long, Document> documentById;
    SecondaryIndex<String, Long, Document> documentByUrl;
    PrimaryIndex<String, ContentSeen> contentSeenByHash;
    PrimaryIndex<String, UrlSeen> urlSeenByUrl;
    PrimaryIndex<String, RobotsInfo> robotsInfoByDomain;

    PrimaryIndex<String, DomainQueue> domainQueueByDomain;
    PrimaryIndex<Long, CrawlQueue> crawlQueueById;
    SecondaryIndex<String, Long, CrawlQueue> crawlQueueByDomain;
    Set<String> currentlyProcessing;

    public DatabaseEnv(String directory) {
        logger.info("Initializing database environment for " + directory);

        // Create database folder if it doesn't exist.
        if (!Files.exists(Paths.get(directory))) {
            File dbDir = new File(directory);
            if (!dbDir.mkdirs()) {
                logger.error("Can't create database environment folder");
            }
        }

        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            StoreConfig storeConfig = new StoreConfig();

            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            storeConfig.setAllowCreate(true);
            storeConfig.setTransactional(true);

            env = new Environment(new File(directory), envConfig);
            store = new EntityStore(env, "EntityStore", storeConfig);

            SequenceConfig docSequenceConfig = store.getSequenceConfig("docId");
            docSequenceConfig.setCacheSize(1);
            store.setSequenceConfig("docId", docSequenceConfig);

            documentById = store.getPrimaryIndex(Long.class, Document.class);
            documentByUrl = store.getSecondaryIndex(documentById, String.class, "url");
            contentSeenByHash = store.getPrimaryIndex(String.class, ContentSeen.class);
            urlSeenByUrl = store.getPrimaryIndex(String.class, UrlSeen.class);
            robotsInfoByDomain = store.getPrimaryIndex(String.class, RobotsInfo.class);
            domainQueueByDomain = store.getPrimaryIndex(String.class, DomainQueue.class);
            crawlQueueById = store.getPrimaryIndex(Long.class, CrawlQueue.class);
            crawlQueueByDomain = store.getSecondaryIndex(crawlQueueById, String.class, "domain");
            currentlyProcessing = new HashSet<String>();

        } catch (DatabaseException dbe) {
            logger.error("Error opening environment and store");
            logger.error(dbe);
        }
    }

    ///////////////////////////////////////////////////
    // Crawler Methods
    ///////////////////////////////////////////////////

    /**
     * Removes from CQ, adds domain to `currentlyProcessing` set. Returns null if CQ
     * is empty.
     */
    public synchronized String crawlQueueRemoveLeft() {

        Transaction txn = env.beginTransaction(null, null);
        if (isCrawlQueueEmpty()) {
            txn.abort();
            return null;
        }

        CrawlQueue queue = crawlQueueById.get(CrawlQueue.frontId);
        crawlQueueById.delete(CrawlQueue.frontId);
        queue.removeFromFront(); // Id now out of valid range.

        String domain = queue.domain;

        if (currentlyProcessing.contains(domain)) {
            txn.abort();
            throw new IllegalStateException("Removing domain already in processing set");
        }

        currentlyProcessing.add(domain);
        txn.commit();

        logger.debug("Removed from crawl queue: " + queue.domain);
        return queue.domain;
    }

    /**
     * Remove domain from processing if necessary. Adds domain to head of CQ if it
     * is non-empty.
     */
    public synchronized void crawlQueueAddLeft(String domain) {
        crawlQueueAdd(domain, true);
    }

    /**
     * Remove domain from processing if necessary. Adds domain to end of CQ if it is
     * non-empty.
     */
    public synchronized void crawlQueueAddRight(String domain) {
        crawlQueueAdd(domain, false);
    }

    private synchronized void crawlQueueAdd(String domain, boolean addToFront) {
        Transaction txn = env.beginTransaction(null, null);

        if (!currentlyProcessing.contains(domain)) {
            txn.abort();
            logger.error("Trying to add domain when it isn't in currently processing");
        }

        currentlyProcessing.remove(domain);
        if (crawlQueueByDomain.contains(domain)) {
            txn.abort();
            throw new IllegalStateException("Trying to add domain already in queue");
        }

        if (!domainQueueByDomain.get(domain).urls.isEmpty()) {
            CrawlQueue queue = new CrawlQueue(domain, addToFront);
            crawlQueueById.put(queue);
        }

        txn.commit();
        logger.debug("Added to crawl queue: " + domain);
    }

    public synchronized boolean isCrawlQueueEmpty() {
        return CrawlQueue.isEmpty();
    }

    /**
     * Add a new url to the Crawler Queue. Adds to DQ, and creates CQ instance if
     * the domain doesn't exist in CQ or `currentlyProcessing`.
     */
    public synchronized void addUrl(String url) {

        Transaction txn = env.beginTransaction(null, null);

        String domain = null;
        try {
            domain = (new URLInfo(url)).getBaseUrl();
        } catch (MalformedURLException e) {
            txn.abort();
            logger.info("Trying to add malformed url: " + url);
            return;
        }

        // Add to domain queue.
        DomainQueue domainQueue = domainQueueByDomain.get(domain);
        if (domainQueue == null) {
            domainQueue = new DomainQueue(domain);
        }
        domainQueue.urls.add(url);
        domainQueueByDomain.put(domainQueue);

        // Add to crawl queue if not in it already and not in `currentlyProcessing`.
        if (!crawlQueueByDomain.contains(domain) && !currentlyProcessing.contains(domain)) {
            CrawlQueue crawlQueue = new CrawlQueue(domain, false);
            crawlQueueById.put(crawlQueue);
        }

        txn.commit();
        logger.debug("Added url to crawler queue: " + url);
    }

    /**
     * Returns the next url to parse from the specified Domain Queue.
     */
    public synchronized String removeUrl(String domain) {

        Transaction txn = env.beginTransaction(null, null);

        if (!domainQueueByDomain.contains(domain)) {
            txn.abort();
            throw new IllegalStateException("Domain not in Domain Queue");
        }

        DomainQueue domainQueue = domainQueueByDomain.get(domain);

        if (domainQueue.urls.isEmpty()) {
            txn.abort();
            throw new IllegalStateException("Domain Queue empty");
        }

        String nextUrl = domainQueue.urls.remove();
        domainQueueByDomain.put(domainQueue);
        txn.commit();

        logger.debug("Removed url from domain queue: " + nextUrl);
        return nextUrl;
    }

    ///////////////////////////////////////////////////
    // Document Methods
    ///////////////////////////////////////////////////

    public synchronized void addDocument(String url, String documentContents, String contentType) {

        Transaction txn = env.beginTransaction(null, null);
        if (documentByUrl.contains(url)) {
            txn.abort();
            throw new IllegalArgumentException("Document already exists in database.");
        }
        Document doc = new Document(url, documentContents, contentType);
        documentById.put(doc);
        txn.commit();

        logger.info("Added content to document: " + doc.id);
    }

    ///////////////////////////////////////////////////
    // Content Seen Methods
    ///////////////////////////////////////////////////

    public synchronized void addContentSeen(String hash) {

        Transaction txn = env.beginTransaction(null, null);
        if (containsHashContent(hash)) {
            txn.abort();
            throw new IllegalArgumentException("Hash already exists in database.");
        }

        ContentSeen content = new ContentSeen(hash);
        contentSeenByHash.put(content);
        txn.commit();

        logger.debug("Added document hash to content seen");
    }

    public synchronized boolean containsHashContent(String hash) {
        return contentSeenByHash.contains(hash);
    }

    ///////////////////////////////////////////////////
    // Url Seen Methods
    ///////////////////////////////////////////////////

    public synchronized void addUrlSeen(String url) {

        Transaction txn = env.beginTransaction(null, null);
        if (containsUrl(url)) {
            txn.abort();
            throw new IllegalArgumentException("Url already exists in database.");
        }

        UrlSeen urlSeen = new UrlSeen(url);
        urlSeenByUrl.put(urlSeen);
        txn.commit();

        logger.debug("Added url to url seen: " + url);
    }

    public synchronized boolean containsUrl(String url) {
        return urlSeenByUrl.contains(url);
    }

    ///////////////////////////////////////////////////
    // Robots Methods
    ///////////////////////////////////////////////////

    public synchronized RobotsInfo addRobotsInfo(String baseUrl, String robotsFile) {

        Transaction txn = env.beginTransaction(null, null);
        if (containsRobotsInfo(baseUrl)) {
            txn.abort();
            throw new IllegalArgumentException("robots.txt already exists for the domain: " + baseUrl);
        }

        RobotsInfo robots = new RobotsInfo(baseUrl, robotsFile);
        robotsInfoByDomain.put(robots);
        txn.commit();

        logger.debug("Added robots.txt contents to robots db: " + baseUrl);
        return robots;
    }

    public synchronized RobotsInfo getRobotsInfo(String baseUrl) {
        return robotsInfoByDomain.get(baseUrl);
    }

    public synchronized boolean containsRobotsInfo(String baseUrl) {
        return robotsInfoByDomain.contains(baseUrl);
    }

    public synchronized void accessDomain(RobotsInfo robots) {

        Transaction txn = env.beginTransaction(null, null);
        robots.access();
        robotsInfoByDomain.put(robots);
        txn.commit();

        logger.debug("Updated last access time in robots db for: " + robots.domain + " to " + robots.lastAccessedTime);
    }

    ///////////////////////////////////////////////////
    // Database Methods
    ///////////////////////////////////////////////////

    public synchronized void flushCurrentlyProcessing() {
        Transaction txn = env.beginTransaction(null, null);
        for (String domain : currentlyProcessing) {

            if (!domainQueueByDomain.get(domain).urls.isEmpty()) {
                CrawlQueue queue = new CrawlQueue(domain, false);
                crawlQueueById.put(queue);
            }

        }
        currentlyProcessing.clear();
        txn.commit();
    }

    public synchronized void close() {
        flushCurrentlyProcessing();
        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                logger.error("Error closing store");
                logger.error(dbe);
            }
        }
        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException dbe) {
                logger.error("Error closing database environment");
                logger.error(dbe);
            }
        }
    }

    public synchronized String toString() {

        // Print documents
        // EntityCursor<Document> documents = documentById.entities();
        // int count = 0;
        String res = "=======================================\n";

        // for (Document document : documents) {
        // res += document;
        // count++;
        // if (count >= 10) {
        // break;
        // }
        // }
        // documents.close();
        res += "Number of documents: " + documentById.count() + "\n";
        res += "=======================================\n";

        // // Print content seen
        // EntityCursor<ContentSeen> contentSeen = contentSeenByHash.entities();
        // count = 0;

        // for (ContentSeen content : contentSeen) {
        // res += content;
        // count++;
        // }
        // contentSeen.close();
        res += "Number of hash contents seen: " + contentSeenByHash.count() + "\n";
        res += "=======================================\n";

        // Print urls seen
        // EntityCursor<UrlSeen> urls = urlSeenByUrl.entities();
        // count = 0;

        // for (UrlSeen url : urls) {
        // res += url;
        // count++;
        // if (count >= 10) {
        // break;
        // }
        // }
        // urls.close();
        res += "Number of urls seen: " + urlSeenByUrl.count() + "\n";
        res += "=======================================\n";

        // // Print robots
        // EntityCursor<RobotsInfo> robots = robotsInfoByDomain.entities();
        // count = 0;

        // for (RobotsInfo robot : robots) {
        // res += robot;
        // count++;
        // }
        // robots.close();
        res += "Number of robots seen: " + robotsInfoByDomain.count() + "\n";
        res += "=======================================\n";

        // Print Domain Queue
        EntityCursor<DomainQueue> domains = domainQueueByDomain.entities();
        int count = 0;
        for (DomainQueue domainQueue : domains) {
            count += domainQueue.size();
        }
        domains.close();
        res += "Number of total domains: " + domainQueueByDomain.count() + "\n";
        res += "Number of urls in queue: " + count + "\n";
        res += "=======================================\n";

        // Print Crawl Queue
        res += "CrawlQueue[frontId=" + CrawlQueue.frontId + ",endId=" + CrawlQueue.endId + "]\n";
        res += "Number of domains in queue: " + crawlQueueById.count() + "\n";
        res += "Number of domains in processing: " + currentlyProcessing.size() + "\n";
        res += "=======================================\n";

        return res;
    }
}
