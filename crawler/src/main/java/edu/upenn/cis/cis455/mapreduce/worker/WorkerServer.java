package edu.upenn.cis.cis455.mapreduce.worker;

import static spark.Spark.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.TopologyContext.STATE;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import spark.Spark;

/**
 * Simple listener for worker creation
 *
 * @author zives
 *
 */
public class WorkerServer {
    static Logger log = LogManager.getLogger(WorkerServer.class);

    static DistributedCluster cluster = new DistributedCluster();

    List<TopologyContext> contexts = new ArrayList<>();

    static List<String> topologies = new ArrayList<>();

    static AtomicBoolean isRunning = new AtomicBoolean(true);
    static Thread workerStatusThread;

    Config currConfig;

    public WorkerServer(int myPort, String storageDir, String masterAddr) throws MalformedURLException {
        log.info("Creating server listener at socket " + myPort);

        port(myPort);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        Spark.post("/definejob", (req, res) -> {
            synchronized (topologies) {
                for (String topo : topologies)
                    cluster.killTopology(topo);
            }
            cluster = new DistributedCluster();

            WorkerJob workerJob;
            try {
                workerJob = om.readValue(req.body(), WorkerJob.class);
                Config config = workerJob.getConfig();

                config.put("storageDirectory", storageDir);

                try {
                    log.info("Processing job definition request " + config.get("job") + " on machine "
                            + config.get("workerIndex"));
                    contexts.add(cluster.submitTopology(config.get("job"), config, workerJob.getTopology()));

                    // Add a new topology
                    synchronized (topologies) {
                        topologies.add(config.get("job"));
                    }

                    currConfig = config;
                } catch (ClassNotFoundException e) {
                    log.error(e.getStackTrace());
                }
                return "Job launched";
            } catch (IOException e) {
                log.error(e.getStackTrace());

                // Internal server error
                res.status(500);
                return e.getMessage();
            }
        });

        Spark.post("/runjob", (req, res) -> {
            log.info("Starting job!");

            cluster.startTopology();

            return "Started";
        });

        Spark.post("/pushdata/:stream", (req, res) -> {
            try {
                String stream = req.params(":stream");
                log.debug("Worker received: " + req.body());
                Tuple tuple = om.readValue(req.body(), Tuple.class);

                log.debug("Worker received: " + tuple + " for " + stream);

                // Find the destination stream and route to it
                StreamRouter router = cluster.getStreamRouter(stream);

                if (contexts.isEmpty())
                    log.error("No topology context -- were we initialized??");

                TopologyContext ourContext = contexts.get(contexts.size() - 1);

                if (!tuple.isEndOfStream()) {
                    // Instrumentation for tracking progress
                    ourContext.incSendOutputs(router.getKey(tuple.getValues()));

                    router.executeLocally(tuple, ourContext, tuple.getSourceExecutor());
                } else {
                    router.executeEndOfStreamLocally(ourContext, tuple.getSourceExecutor());
                }

                return "OK";
            } catch (IOException e) {
                e.printStackTrace();

                res.status(500);
                return e.getMessage();
            }

        });

        Spark.get("/shutdown", (request, response) -> {
            log.info("Shutting down");
            isRunning.set(false);
            return "Shutdown has started!";
        });

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
                log.info("worker status thread running");

                // Send workerstatus to master server.
                try {
                    String job = currConfig != null ? currConfig.get("job") : "None";
                    String status = "IDLE";
                    int keysRead = 0;
                    int keysWritten = 0;

                    if (!contexts.isEmpty()) {
                        TopologyContext context = contexts.get(contexts.size() - 1);
                        STATE state = context.getState();
                        status = state.toString();
                        if (state == STATE.IDLE || state == STATE.REDUCING) {
                            keysWritten = context.getReduceOutputs();
                        } else if (state == STATE.WAITING || state == STATE.MAPPING)
                            keysWritten = context.getMapOutputs();
                    }

                    if (status.equals("IDLE")) {
                        keysRead = 0;
                    }

                    String queryString = "?port=" + myPort + "&status=" + status + "&job=" + job + "&keysRead="
                            + keysRead + "&keysWritten=" + keysWritten + "&results=" + getResults();
                    log.info("Sending worker status to master server with parameters: " + queryString);

                    URL url = new URL(baseUrl + queryString);
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.getInputStream();
                    conn.disconnect();
                } catch (IOException e) {
                    log.info(e);
                }

                // Sleep for 10 seconds.
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        };
        workerStatusThread = new Thread(workerStatusRunnable);
        workerStatusThread.start();
    }

    public static void createWorker(Map<String, String> config) {
        if (!config.containsKey("workerList"))
            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

        if (!config.containsKey("workerIndex")) {
            throw new RuntimeException("Worker spout doesn't know its worker ID");
        } else {
            String[] addresses = WorkerHelper.getWorkers(config);
            String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

            log.debug("Initializing worker " + myAddress);

            URL url;
            try {
                url = new URL(myAddress);

                String defaultStorageDir = "storage/node" + config.get("workerIndex");
                String defaultMasterAddr = "localhost:45555";
                new WorkerServer(url.getPort(), defaultStorageDir, defaultMasterAddr);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void shutdown() {
        synchronized (topologies) {
            for (String topo : topologies)
                cluster.killTopology(topo);
        }

        cluster.shutdown();
        Spark.stop();
        try {
            workerStatusThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getResults() throws UnsupportedEncodingException {
        if (currConfig == null) {
            return "[]";
        }

        String results = "";
        BufferedReader reader = null;
        try {
            String filename = currConfig.get("storageDirectory") + currConfig.get("outputDirectory") + "output.txt";
            reader = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e) {
            log.error("Output file not created yet or not found.\n" + e.getStackTrace());
            return "[]";
        }

        try {
            String line = reader.readLine();
            int i = 0;

            while (line != null && i < 100) {
                results += line + ",";
                line = reader.readLine();
                i++;
            }
        } catch (IOException e) {
            log.error(e.getStackTrace());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        if (results.length() > 0 && results.charAt(results.length() - 1) == ',') {
            results = results.substring(0, results.length() - 1);
        }
        results = URLEncoder.encode(results, StandardCharsets.UTF_8.toString());
        return "[" + results + "]";
    }

    /**
     * Simple launch for worker server. Note that you may want to change / replace
     * most of this.
     *
     * @param args
     * @throws MalformedURLException
     */
    public static void main(String args[]) throws MalformedURLException {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.ALL);

        if (args.length < 3) {
            System.out.println("Usage: WorkerServer [port number] [master host/IP]:[master port] [storage directory]");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        String masterAddr = args[1];
        String storageDir = args[2];
        if (storageDir.charAt(storageDir.length() - 1) != '/') {
            storageDir += "/";
        }

        log.info("Worker node startup, on port " + myPort);

        new WorkerServer(myPort, storageDir, masterAddr);
    }
}
