package edu.upenn.cis.cis455.mapreduce.master;

import static spark.Spark.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.OutputBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.spout.InputSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.cis455.mapreduce.master.MasterServer;

public class MasterServer {
    static Logger log = LogManager.getLogger(MasterServer.class);

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    static List<String> workers = new ArrayList<String>();
    static Map<String, WorkerData> workerLookup = new HashMap<String, WorkerData>();
    static AtomicBoolean isRunning;

    public static void registerStatusPage() {
        get("/status", (request, response) -> {
            response.type("text/html");

            return ("<html><head><title>Master</title></head>\n"
                    + "<body><h2>Personal Info</h2><div>Name: Andy Xiao <br>SEAS Login: andyxiao</div>"
                    + "<h2>Worker Status Info</h2>" + "<div>" + getStatusString() + "</div>" + "<br><h2>Submit Job</h2>"
                    + "<form method=\"POST\" action=\"/submitjob\">\r\n"
                    + "Job Name: <input type=\"text\" name=\"jobname\"/><br/>\r\n"
                    + "Class Name: <input type=\"text\" name=\"classname\"/><br/>\r\n"
                    + "Input Directory: <input type=\"text\" name=\"input\"/><br/>\r\n"
                    + "Output Directory: <input type=\"text\" name=\"output\"/><br/>\r\n"
                    + "Map Threads: <input type=\"text\" name=\"map\"/><br/>\r\n"
                    + "Reduce Threads: <input type=\"text\" name=\"reduce\"/><br/>\r\n"
                    + "<input type=\"submit\" value=\"Submit\"></form>" + "</body></html>");
        });
    }

    /**
     * The mainline for launching a MapReduce Master. This should handle at least
     * the status and workerstatus routes, and optionally initialize a worker as
     * well.
     *
     * @param args
     */
    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.ALL);

        if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        port(myPort);
        isRunning = new AtomicBoolean(true);

        log.info("Master node startup, on port " + myPort);

        registerStatusPage();

        post("/submitjob", (request, response) -> {
            log.info("Received submit job " + request.queryParams("jobname"));

            String inputDir = request.queryParams("input");
            String outputDir = request.queryParams("output");
            if (inputDir.charAt(inputDir.length() - 1) != '/') {
                inputDir += "/";
            }
            if (outputDir.charAt(outputDir.length() - 1) != '/') {
                outputDir += "/";
            }

            // Build config from the passed in data.
            Config config = new Config();
            config.put("workerList", getWorkerList());
            config.put("job", request.queryParams("jobname"));
            config.put("mapClass", request.queryParams("classname"));
            config.put("reduceClass", request.queryParams("classname"));
            config.put("spoutExecutors", "1");
            config.put("mapExecutors", request.queryParams("map"));
            config.put("reduceExecutors", request.queryParams("reduce"));
            config.put("inputDirectory", inputDir);
            config.put("outputDirectory", outputDir);

            // Build topology.
            FileSpout fileSpout = new InputSpout();
            MapBolt mapBolt = new MapBolt();
            ReduceBolt reduceBolt = new ReduceBolt();
            OutputBolt printer = new OutputBolt();
            TopologyBuilder builder = new TopologyBuilder();

            Integer numSpoutExecutors = null;
            Integer numMapExecutors = null;
            Integer numReduceExecutors = null;
            try {
                numSpoutExecutors = Integer.valueOf(config.get("spoutExecutors"));
                numMapExecutors = Integer.valueOf(config.get("mapExecutors"));
                numReduceExecutors = Integer.valueOf(config.get("reduceExecutors"));
            } catch (NumberFormatException e) {
                halt(400, "Expected number for Map/Reduce threads.");
            }

            builder.setSpout(WORD_SPOUT, fileSpout, numSpoutExecutors);
            builder.setBolt(MAP_BOLT, mapBolt, numMapExecutors).fieldsGrouping(WORD_SPOUT, new Fields("value"));
            builder.setBolt(REDUCE_BOLT, reduceBolt, numReduceExecutors).fieldsGrouping(MAP_BOLT, new Fields("key"));
            builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);

            Topology topo = builder.createTopology();
            WorkerJob job = new WorkerJob(topo, config);

            // Send job to workers.
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
            try {
                String[] workers = WorkerHelper.getWorkers(config);

                int i = 0;
                for (String dest : workers) {
                    config.put("workerIndex", String.valueOf(i++));
                    if (sendJob(dest, "POST", config, "definejob",
                            mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
                                    .getResponseCode() != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job definition request failed");
                    }
                }
                for (String dest : workers) {
                    if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job execution request failed");
                    }
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                System.exit(0);
            }

            return "Job submitted!";
        });

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

        get("/shutdown", (request, response) -> {
            log.info("Shutting down");

            synchronized (workerLookup) {
                for (String addr : workers) {
                    URL url = new URL("http://" + addr + "/shutdown");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.getInputStream();
                    conn.disconnect();
                }
            }

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
            stop();
        };
        Thread shutdownThread = new Thread(shutdownRunnable);
        shutdownThread.start();
    }

    private static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters)
            throws IOException {
        URL url = new URL(dest + "/" + job);

        log.info("Sending request to " + url.toString());

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);

        if (reqType.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
        } else {
            conn.getOutputStream();
        }

        return conn;
    }

    private static String getWorkerList() {
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

    private static String getStatusString() {
        int i = 0;
        String res = "";
        synchronized (workerLookup) {
            for (String addr : workers) {
                WorkerData data = workerLookup.get(addr);

                if (data.isActive()) {
                    res += (i++) + ": " + data + "<br>";
                }
            }
        }
        return res;
    }
}