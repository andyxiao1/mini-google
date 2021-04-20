package edu.upenn.cis.cis455.crawler.master;

import static spark.Spark.*;
import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.streamprocessors.DocumentFetcherBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.LinkExtractorBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.LinkFilterBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.UrlSpout;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CrawlMaster {
    static Logger log = LogManager.getLogger(CrawlMaster.class);

    String startUrl;
    Integer maxDocSize;
    Integer maxCrawlCount;

    List<String> workers = new ArrayList<String>();
    // Map<String, WorkerData> workerLookup = new HashMap<String,
    // WorkerData>();
    AtomicBoolean isRunning = new AtomicBoolean(true);

    public CrawlMaster(int port, String url, Integer maxSize, Integer count) {
        log.info("Crawl master node startup, on port " + port);

        port(port);
        startUrl = url;
        maxDocSize = maxSize;
        maxCrawlCount = count;

        defineStartCrawlRoute();
        defineShutdownRoute();
        setupShutdownThread();
        // TODO: worker status route - also connects + maintains worker list
    }

    private void defineStartCrawlRoute() {
        get("/startcrawl", (request, response) -> {
            log.info("Received crawl start command");

            // Setup StormLite topology.
            Config config = new Config();
            config.put(MAX_DOCUMENT_SIZE, maxDocSize.toString());
            config.put(CRAWL_COUNT, maxCrawlCount.toString());
            config.put(WORKER_LIST, "[127.0.0.1:8001,127.0.0.1:8002]"); // TODO: hardcoded
            // config.put(WORKER_LIST, "[127.0.0.1:8001]"); // TODO: hardcoded

            UrlSpout urlSpout = new UrlSpout();
            DocumentFetcherBolt docFetcherBolt = new DocumentFetcherBolt();
            LinkExtractorBolt linkExtractorBolt = new LinkExtractorBolt();
            LinkFilterBolt linkFilterBolt = new LinkFilterBolt();
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout(URL_SPOUT, urlSpout, 3);
            builder.setBolt(DOC_FETCHER_BOLT, docFetcherBolt, 3).fieldsGrouping(URL_SPOUT, new Fields("url"));
            builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, 3).fieldsGrouping(DOC_FETCHER_BOLT,
                    new Fields("url"));
            builder.setBolt(LINK_FILTER_BOLT, linkFilterBolt, 3).fieldsGrouping(LINK_EXTRACTOR_BOLT, new Fields("url"));

            Topology topo = builder.createTopology();
            WorkerJob job = new WorkerJob(topo, config);

            // Send job to workers.
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
            try {
                String[] workers = WorkerHelper.getWorkers(config);

                for (int i = 0; i < workers.length; i++) {
                    String dest = workers[i];
                    config.put(WORKER_INDEX, String.valueOf(i));
                    // TODO: hardcoded
                    if (i == 0) {
                        config.put(START_URL, startUrl); // TODO: hardcoded, make into list
                    } else {
                        config.put(START_URL, "https://en.wikipedia.org/wiki/Patience_sorting");
                    }

                    String url = dest + "/" + "initcrawl";
                    String body = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job);
                    if (HTTP.sendData(url, POST_REQUEST, body) != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job definition request failed");
                    }
                }

                for (String dest : workers) {
                    String url = dest + "/" + "startcrawl";
                    if (HTTP.sendData(url, POST_REQUEST, "") != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job execution request failed");
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }

            return "Started!";
        });
    }

    private void defineShutdownRoute() {
        get("/shutdown", (request, response) -> {
            log.info("Shutting down");

            // TODO: hardcoded
            String addr = "127.0.0.1:8001";
            HTTP.sendData("http://" + addr + "/shutdown", GET_REQUEST, "");
            addr = "127.0.0.1:8002";
            HTTP.sendData("http://" + addr + "/shutdown", GET_REQUEST, "");

            isRunning.set(false);
            return "Shutdown has started!";
        });
    }

    private void setupShutdownThread() {
        // Background thread to check for shutdown.
        Runnable shutdownRunnable = () -> {
            while (isRunning.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            stop();
        };
        Thread shutdownThread = new Thread(shutdownRunnable);
        shutdownThread.start();
    }

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.ALL);

        // Process arguments.
        if (args.length != 4) {
            System.out.println(
                    "Usage: CrawlMaster {port number} {start URL} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }

        // TODO: start url should prob be a file of seed urls, for now we hardcode
        int port = Integer.valueOf(args[0]);
        String startUrl = args[1];
        Integer maxDocSize = Integer.valueOf(args[2]);
        Integer maxCrawlCount = Integer.valueOf(args[3]);

        // Start CrawlMaster server.
        new CrawlMaster(port, startUrl, maxDocSize, maxCrawlCount);
    }
}
