package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

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

public class DatabaseEnv implements StorageInterface {

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

    @Override
    public int getCorpusSize() {
        return (int) documentById.count();
    }

    @Override
    public int addDocument(String url, String documentContents) {
        return addDocument(url, documentContents, "");
    }

    public int addDocument(String url, String documentContents, String contentType) {

        Transaction txn = env.beginTransaction(null, null);
        Document doc = documentByUrl.get(url);
        if (doc != null) {
            doc.setContent(documentContents);
            logger.info("Updating document: " + doc.id);
        } else {
            doc = new Document(url, documentContents);
        }
        doc.updateLastFetchedDate();
        doc.setContentType(contentType);
        documentById.put(doc);
        txn.commit();

        logger.info("Added content to document: " + doc.id);
        return doc.id;
    }

    @Override
    public String getDocument(String url) {
        Document doc = documentByUrl.get(url);
        return doc.content;
    }

    public Document getDocument(int id) {
        return documentById.get(id);
    }

    public String getDocumentType(String url) {
        Document doc = documentByUrl.get(url);
        return doc.contentType;
    }

    public long getDocumentLastFetch(String url) {
        if (!containsDocument(url)) {
            return -1;
        }
        Document doc = documentByUrl.get(url);
        return doc.lastFetchedDate;
    }

    public boolean containsDocument(String url) {
        if (url == null) {
            return false;
        }
        return documentByUrl.contains(url);
    }

    ///////////////////////////////////////////////////
    // Content Seen Methods
    ///////////////////////////////////////////////////

    public void addContentSeen(String hash) {

        Transaction txn = env.beginTransaction(null, null);
        if (containsHashContent(hash)) {
            txn.abort();
            throw new IllegalArgumentException("Hash already exists in database.");
        }

        ContentSeen content = new ContentSeen(hash);
        contentSeenByHash.put(content);
        txn.commit();

        logger.info("Added document hash to content seen");
    }

    public boolean containsHashContent(String hash) {
        return contentSeenByHash.contains(hash);
    }

    ///////////////////////////////////////////////////
    // Url Seen Methods
    ///////////////////////////////////////////////////

    public void addUrl(String url) {

        Transaction txn = env.beginTransaction(null, null);
        if (containsUrl(url)) {
            txn.abort();
            throw new IllegalArgumentException("Url already exists in database.");
        }

        UrlSeen urlSeen = new UrlSeen(url);
        urlSeenByUrl.put(urlSeen);
        txn.commit();

        logger.info("Added url to url seen: " + url);
    }

    public boolean containsUrl(String url) {
        return urlSeenByUrl.contains(url);
    }

    ///////////////////////////////////////////////////
    // Robots Methods
    ///////////////////////////////////////////////////

    public RobotsInfo addRobotsInfo(String baseUrl, String robotsFile) {

        Transaction txn = env.beginTransaction(null, null);
        if (containsRobotsInfo(baseUrl)) {
            txn.abort();
            throw new IllegalArgumentException("robots.txt already exists for the domain: " + baseUrl);
        }

        RobotsInfo robots = new RobotsInfo(baseUrl, robotsFile);
        robotsInfoByDomain.put(robots);
        txn.commit();

        logger.info("Added robots.txt contents to robots db: " + baseUrl);
        return robots;
    }

    public RobotsInfo getRobotsInfo(String baseUrl) {
        return robotsInfoByDomain.get(baseUrl);
    }

    public boolean containsRobotsInfo(String baseUrl) {
        return robotsInfoByDomain.contains(baseUrl);
    }

    public void accessDomain(RobotsInfo robots) {

        Transaction txn = env.beginTransaction(null, null);
        robots.access();
        robotsInfoByDomain.put(robots);
        txn.commit();

        logger.info("Updated last access time in robots db for: " + robots.domain + " to " + robots.lastAccessedTime);
    }

    ///////////////////////////////////////////////////
    // Çrawler Methods
    ///////////////////////////////////////////////////

    public void resetRun() {
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

    @Override
    public void close() {
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

    public String toString() {

        // Print documents
        EntityCursor<Document> documents = documentById.entities();
        int count = 0;
        String res = "=======================================\n";

        for (Document document : documents) {
            res += document;
            count++;
        }
        documents.close();
        res += "Number of documents: " + count + "\n";
        res += "=======================================\n";

        // Print content seen
        EntityCursor<ContentSeen> contentSeen = contentSeenByHash.entities();
        count = 0;

        for (ContentSeen content : contentSeen) {
            res += content;
            count++;
        }
        contentSeen.close();
        res += "Number of hash contents seen: " + count + "\n";
        res += "=======================================\n";

        // Print urls seen
        EntityCursor<UrlSeen> urls = urlSeenByUrl.entities();
        count = 0;

        for (UrlSeen url : urls) {
            res += url;
            count++;
        }
        urls.close();
        res += "Number of urls seen: " + count + "\n";
        res += "=======================================\n";

        // Print robots
        EntityCursor<RobotsInfo> robots = robotsInfoByDomain.entities();
        count = 0;

        for (RobotsInfo robot : robots) {
            res += robot;
            count++;
        }
        robots.close();
        res += "Number of robots seen: " + count + "\n";
        res += "=======================================\n";

        return res;
    }
}
