package edu.upenn.cis.cis455.crawler.worker;

import static spark.Spark.*;
import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.CrawlerQueue;
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

    DistributedCluster cluster;
    ObjectMapper om;
    DatabaseEnv database;
    CrawlerQueue queue;
    TopologyContext context;

    AtomicBoolean isRunning = new AtomicBoolean(true);

    public CrawlWorker(int port, String masterAddress, String storageDirectory) {
        log.info("Crawl worker node startup, on port" + port);

        port(port);
        masterAddr = masterAddress;
        storageDir = storageDirectory;
        cluster = new DistributedCluster();
        om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        database = (DatabaseEnv) StorageFactory.getDatabaseInstance(storageDir);
        queue = CrawlerQueue.getSingleton();
        System.out.println(database);
        database.resetRun();

        defineInitCrawlRoute();
        defineStartCrawlRoute();
        definePushDataRoute();
        defineShutdownRoute();
        setupShutdownThread();
    }

    private void defineInitCrawlRoute() {
        post("/initcrawl", (request, response) -> {
            WorkerJob workerJob;
            try {
                workerJob = om.readValue(request.body(), WorkerJob.class);
                Config config = workerJob.getConfig();
                config.put(DATABASE_DIRECTORY, storageDir);

                String startUrl = config.get(START_URL);
                database.addUrl(startUrl);
                queue.addUrl(startUrl);

                log.info("Processing init crawl request on machine " + config.get(WORKER_INDEX));
                context = cluster.submitTopology("Crawler", config, workerJob.getTopology());

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

                // TODO: we don't need this bc we don't have any notion of EOS anymore??
                // if (!tuple.isEndOfStream()) {
                // // Instrumentation for tracking progress
                // context.incSendOutputs(router.getKey(tuple.getValues()));

                // router.executeLocally(tuple, context, tuple.getSourceExecutor());
                // } else {
                // router.executeEndOfStreamLocally(context, tuple.getSourceExecutor());
                // }

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

    private void shutdown() {
        System.out.println(database);
        cluster.killTopology("");
        cluster.shutdown();
        stop();
    }

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.ALL);

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
