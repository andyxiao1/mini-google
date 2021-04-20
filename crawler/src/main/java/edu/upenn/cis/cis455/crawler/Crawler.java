package edu.upenn.cis.cis455.crawler;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.streamprocessors.DocumentFetcherBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.LinkExtractorBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.LinkFilterBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.UrlSpout;
import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Crawler {

    static final Logger logger = LogManager.getLogger(Crawler.class);

    /**
     * Main program: init database, start crawler, wait for it to notify that it is
     * done, then close.
     */
    public static void main(String args[]) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.ALL);

        ///////// Process arguments
        if (args.length < 3 || args.length > 5) {
            System.out.println(
                    "Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }

        logger.info("Crawler starting");

        String startUrl = args[0];
        String envPath = args[1];
        Integer size = Integer.valueOf(args[2]);
        Integer count = args.length == 4 ? Integer.valueOf(args[3]) : 100;

        if (!Files.exists(Paths.get(envPath))) {
            try {
                Files.createDirectory(Paths.get(envPath));
            } catch (IOException e) {
                logger.error("Can't create database environment folder");
                logger.error(e);
            }
        }

        DatabaseEnv db = (DatabaseEnv) StorageFactory.getDatabaseInstance(envPath);
        CrawlerQueue queue = CrawlerQueue.getSingleton();

        ///////// Setup StormLite topology
        Config config = new Config();
        config.put(DATABASE_DIRECTORY, envPath);
        config.put(MAX_DOCUMENT_SIZE, size.toString());
        config.put(CRAWL_COUNT, count.toString());
        config.put(WORKER_INDEX, "0"); // Testing
        config.put(WORKER_LIST, "[127.0.0.1:4555]");

        UrlSpout urlSpout = new UrlSpout();
        DocumentFetcherBolt docFetcherBolt = new DocumentFetcherBolt();
        LinkExtractorBolt linkExtractorBolt = new LinkExtractorBolt();
        LinkFilterBolt linkFilterBolt = new LinkFilterBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(URL_SPOUT, urlSpout, 3);

        builder.setBolt(DOC_FETCHER_BOLT, docFetcherBolt, 3).fieldsGrouping(URL_SPOUT, new Fields("url"));
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, 3).fieldsGrouping(DOC_FETCHER_BOLT, new Fields("url"));
        builder.setBolt(LINK_FILTER_BOLT, linkFilterBolt, 3).fieldsGrouping(LINK_EXTRACTOR_BOLT, new Fields("url"));

        DistributedCluster cluster = new DistributedCluster();
        Topology topo = builder.createTopology();

        ///////// Execute crawl
        db.resetRun();
        db.addUrl(startUrl);
        queue.addUrl(startUrl);

        logger.info("Starting crawl of " + count + " documents, starting at " + startUrl);
        try {
            cluster.submitTopology(CLUSTER_TOPOLOGY, config, topo);
            cluster.startTopology();
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        while (!CrawlerState.isFinished) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.error("Sleep call interupted");
                logger.error(e);
            }
        }
        System.out.println(db);

        logger.info("Done crawling!");
        cluster.killTopology(CLUSTER_TOPOLOGY);
        cluster.shutdown();

        db.close();
        System.exit(0);
    }
}