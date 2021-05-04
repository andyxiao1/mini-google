package edu.upenn.cis.cis455.crawler.worker;

import static spark.Spark.*;
import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.crawler.utils.HTTP;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class CrawlWorker {
    static Logger log = LogManager.getLogger(CrawlWorker.class);

    String masterAddr;
    String storageDir;
    int port;

    DistributedCluster cluster;
    ObjectMapper om;
    DatabaseEnv database;
    TopologyContext context;
    Thread workerStatusThread;

    AtomicBoolean isRunning = new AtomicBoolean(true);

    public CrawlWorker(int myPort, String masterAddress, String storageDirectory) {
        log.info("Crawl worker node startup, on port " + myPort);

        port = myPort;
        masterAddr = masterAddress;
        storageDir = storageDirectory;
        cluster = new DistributedCluster();
        om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        database = StorageFactory.getDatabaseInstance(storageDir);
        // database.resetRun();

        port(port);
        defineInitCrawlRoute();
        defineStartCrawlRoute();
        definePushDataRoute();
        defineShutdownRoute();
        setupShutdownThread();
        setupWorkerStatusThread();
    }

    private void defineInitCrawlRoute() {
        post("/initcrawl", (request, response) -> {
            WorkerJob workerJob;
            try {
                workerJob = om.readValue(request.body(), WorkerJob.class);
                Config config = workerJob.getConfig();
                config.put(DATABASE_DIRECTORY, storageDir);
                int crawlThreads = Integer.parseInt(config.get(THREAD_COUNT));

                log.info("Processing init crawl request on machine " + config.get(WORKER_INDEX));
                context = cluster.submitTopology("Crawler", config, workerJob.getTopology(), crawlThreads);

                return "Job launched";
            } catch (ClassNotFoundException | IOException e) {
                log.error(e.getStackTrace());

                // Internal server error
                response.status(500);
                return e.getMessage();
            }
        });
    }

    private void defineStartCrawlRoute() {
        post("/startcrawl", (request, response) -> {
            log.info("Starting crawl!");
            cluster.startTopology();
            return "Started";
        });
    }

    private void definePushDataRoute() {
        post("/pushdata/:stream", (req, res) -> {
            try {
                String stream = req.params(":stream");
                log.debug("Worker received: " + req.body());
                Tuple tuple = om.readValue(req.body(), Tuple.class);

                log.debug("Worker received: " + tuple + " for " + stream);

                // Find the destination stream and route to it
                StreamRouter router = cluster.getStreamRouter(stream);

                if (context == null) {
                    log.error("No topology context -- were we initialized??");
                }

                router.executeLocally(tuple, context, tuple.getSourceExecutor());
                return "OK";
            } catch (IOException e) {
                e.printStackTrace();

                res.status(500);
                return e.getMessage();
            }

        });
    }

    private void defineShutdownRoute() {
        get("/shutdown", (request, response) -> {
            log.info("Shutting down");
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
            shutdown();
        };
        Thread shutdownThread = new Thread(shutdownRunnable);
        shutdownThread.start();
    }

    private void setupWorkerStatusThread() {
        // Background thread to send /workerstatus updates.
        Runnable workerStatusRunnable = () -> {
            String baseUrl = "";
            if (masterAddr.startsWith("localhost:")) {
                baseUrl = "127.0.0.1:" + masterAddr.substring(10);
            } else {
                baseUrl = masterAddr;
            }
            baseUrl = "http://" + baseUrl + "/workerstatus";

            while (isRunning.get()) {
                log.debug("worker status thread running");

                // Send workerstatus to master server.
                try {
                    String queryString = "?port=" + port + "&count=" + CrawlerState.count.get();
                    log.info("Sending worker status to master server with parameters: " + queryString);

                    HTTP.sendData(baseUrl + queryString, GET_REQUEST, "");
                } catch (IOException e) {
                    log.info(e);
                }

                // Sleep for 5 seconds.
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        };
        workerStatusThread = new Thread(workerStatusRunnable);
        workerStatusThread.start();
    }

    private void shutdown() {
        CrawlerState.isShutdown.set(true);
        cluster.killTopology("");
        cluster.shutdown();
        stop();

        System.out.println(database);
        System.out.println("Crawl Count: " + CrawlerState.count.get());
        database.close();

        try {
            workerStatusThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.INFO);

        // Process arguments.
        if (args.length != 3) {
            System.out.println("Usage: CrawlWorker {port number} {master host/IP}:{master port} {storage directory}");
            System.exit(1);
        }

        int port = Integer.valueOf(args[0]);
        String masterAddr = args[1];
        String storageDir = args[2];

        // Start CrawlWorker server.
        new CrawlWorker(port, masterAddr, storageDir);
    }
}
