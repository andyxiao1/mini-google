package edu.upenn.cis.cis455.crawler.master;

import static spark.Spark.*;
import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.streamprocessors.DocumentFetcherBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.LinkExtractorBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.LinkFilterBolt;
import edu.upenn.cis.cis455.crawler.streamprocessors.UrlSpout;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CrawlMaster {
    static Logger log = LogManager.getLogger(CrawlMaster.class);

    String startUrl;
    Integer maxDocSize;
    Integer maxCrawlCount;

    List<String> workers = new ArrayList<String>();
    Map<String, WorkerData> workerLookup = new HashMap<String, WorkerData>();
    AtomicBoolean isRunning = new AtomicBoolean(true);

    public CrawlMaster(int port, String url, Integer maxSize, Integer count) {
        log.info("Crawl master node startup, on port " + port);

        port(port);
        startUrl = url;
        maxDocSize = maxSize;
        maxCrawlCount = count;

        defineStartCrawlRoute();
        defineWorkerStatusRoute();
        defineShutdownRoute();
        setupShutdownThread();
    }

    private void defineStartCrawlRoute() {
        get("/startcrawl", (request, response) -> {
            log.info("Received crawl start command");

            // Setup StormLite topology.
            Config config = new Config();
            config.put(MAX_DOCUMENT_SIZE, maxDocSize.toString());
            config.put(CRAWL_COUNT, maxCrawlCount.toString());
            config.put(WORKER_LIST, getWorkerList());
            WorkerJob job = new WorkerJob(setupTopology(), config);

            // Send job to workers.
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
            try {
                String[] workerAddrs = WorkerHelper.getWorkers(config);

                // Initialize all workers by sending StormLite topology.
                for (int i = 0; i < workerAddrs.length; i++) {
                    String dest = workerAddrs[i];
                    config.put(WORKER_INDEX, String.valueOf(i));

                    String url = dest + "/" + "initcrawl";
                    String body = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job);
                    if (HTTP.sendData(url, POST_REQUEST, body) != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job definition request failed");
                    }
                }

                // Start all crawl workers.
                for (String dest : workerAddrs) {
                    String url = dest + "/" + "startcrawl";
                    if (HTTP.sendData(url, POST_REQUEST, "") != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job execution request failed");
                    }
                }

                // Send each start URL to every worker's `pushdata` link filter bolt route. They
                // will only execute locally if the tuple belongs to them.
                List<String> startUrls = new ArrayList<String>();
                // startUrls.add(startUrl);
                startUrls.add("https://en.wikipedia.org/wiki/Poisson_distribution");
                startUrls.add("https://en.wikipedia.org/wiki/Pittsburgh_Steelers");
                startUrls.add("https://www.imdb.com/title/tt0848228/");
                startUrls.add("https://www.britannica.com/");

                for (String currUrl : startUrls) {
                    String domain = (new URLInfo(currUrl)).getBaseUrl();
                    Values<Object> urlValues = new Values<Object>(domain, currUrl);
                    Tuple urlTuple = new Tuple(new Fields("domain", "url"), urlValues, "master");
                    for (String dest : workerAddrs) {
                        String url = dest + "/" + "pushdata/" + LINK_FILTER_BOLT;
                        String body = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(urlTuple);
                        if (HTTP.sendData(url, POST_REQUEST, body) != HttpURLConnection.HTTP_OK) {
                            throw new RuntimeException("Start url pushdata request failed");
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }

            return "Started!";
        });
    }

    private void defineWorkerStatusRoute() {
        get("/workerstatus", (request, response) -> {
            log.info("Received worker status from " + request.ip() + " with parameters: " + request.queryString());

            String addr = request.ip() + ":" + request.queryParams("port");
            synchronized (workerLookup) {
                if (workerLookup.containsKey(addr)) {
                    log.info("Updating worker " + addr);
                    workerLookup.get(addr).update(request);
                } else {
                    log.info("Adding new worker " + addr);
                    workers.add(addr);
                    workerLookup.put(addr, new WorkerData(request));
                }
            }
            return "Status updated!";
        });
    }

    private void defineShutdownRoute() {
        get("/shutdown", (request, response) -> {
            log.info("Shutting down");

            synchronized (workerLookup) {
                for (String addr : workers) {
                    HTTP.sendData("http://" + addr + "/shutdown", GET_REQUEST, "");
                }
            }

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

    private String getWorkerList() {
        synchronized (workerLookup) {
            String result = "";
            for (String addr : workers) {
                if (workerLookup.get(addr).isActive()) {
                    result += addr + ",";
                }
            }

            if (result.length() > 0 && result.charAt(result.length() - 1) == ',') {
                result = result.substring(0, result.length() - 1);
            }

            // Example: [127.0.0.1:8001,127.0.0.1:8002]
            log.info("Building workerList [" + result + "]");
            return "[" + result + "]";
        }
    }

    private Topology setupTopology() {
        // Setup StormLite topology.
        UrlSpout urlSpout = new UrlSpout();
        DocumentFetcherBolt docFetcherBolt = new DocumentFetcherBolt();
        LinkExtractorBolt linkExtractorBolt = new LinkExtractorBolt();
        LinkFilterBolt linkFilterBolt = new LinkFilterBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(URL_SPOUT, urlSpout, 2);
        builder.setBolt(DOC_FETCHER_BOLT, docFetcherBolt, 5).fieldsGrouping(URL_SPOUT, new Fields("domain"));
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, 2).fieldsGrouping(DOC_FETCHER_BOLT,
                new Fields("domain"));
        builder.setBolt(LINK_FILTER_BOLT, linkFilterBolt, 5).fieldsGrouping(LINK_EXTRACTOR_BOLT, new Fields("domain"));

        return builder.createTopology();
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
