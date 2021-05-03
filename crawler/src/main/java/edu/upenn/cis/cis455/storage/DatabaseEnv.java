package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabaseEnv {

    static final Logger logger = LogManager.getLogger(DatabaseEnv.class);

    Environment env;
    EntityStore store;
    PrimaryIndex<Integer, Document> documentById;
    SecondaryIndex<String, Integer, Document> documentByUrl;
    PrimaryIndex<String, ContentSeen> contentSeenByHash;
    PrimaryIndex<String, UrlSeen> urlSeenByUrl;
    PrimaryIndex<String, RobotsInfo> robotsInfoByDomain;

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

            documentById = store.getPrimaryIndex(Integer.class, Document.class);
            documentByUrl = store.getSecondaryIndex(documentById, String.class, "url");
            contentSeenByHash = store.getPrimaryIndex(String.class, ContentSeen.class);
            urlSeenByUrl = store.getPrimaryIndex(String.class, UrlSeen.class);
            robotsInfoByDomain = store.getPrimaryIndex(String.class, RobotsInfo.class);

        } catch (DatabaseException dbe) {
            logger.error("Error opening environment and store");
            logger.error(dbe);
        }
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

    public synchronized void addUrl(String url) {

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
    // Çrawler Methods
    ///////////////////////////////////////////////////

    public synchronized void resetRun() {
        store.truncateClass(ContentSeen.class);
        contentSeenByHash = store.getPrimaryIndex(String.class, ContentSeen.class);

        store.truncateClass(UrlSeen.class);
        urlSeenByUrl = store.getPrimaryIndex(String.class, UrlSeen.class);

        store.truncateClass(RobotsInfo.class);
        robotsInfoByDomain = store.getPrimaryIndex(String.class, RobotsInfo.class);
    }

    ///////////////////////////////////////////////////
    // Database Methods
    ///////////////////////////////////////////////////

    public synchronized void close() {
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

        return res;
    }
}
