package edu.upenn.cis.cis455.mapreduce.master;

import java.time.Instant;

import spark.Request;

/**
 * POJO for Worker Data.
 */
public class WorkerData {
    long lastActive;
    String ip;
    String port;
    String status;
    String job;
    String keysRead;
    String keysWritten;
    String results;

    public WorkerData(Request request) {
        ip = request.ip();
        port = request.queryParams("port");
        update(request);
    }

    public void update(Request request) {
        if (!request.ip().equals(ip) || !request.queryParams("port").equals(port)) {
            throw new IllegalArgumentException("Trying to update wrong WorkerData.");
        }
        status = request.queryParams("status");
        job = request.queryParams("job");
        keysRead = request.queryParams("keysRead");
        keysWritten = request.queryParams("keysWritten");
        results = request.queryParams("results");

        lastActive = Instant.now().toEpochMilli();
    }

    public boolean isActive() {
        long currTime = Instant.now().toEpochMilli();
        return currTime - lastActive <= 30000;
    }

    @Override
    public String toString() {
        // Example: port=8002, status=IDLE, job=Foo, keysRead=2, keysWritten=2,
        // results=[(a, 1),(b,1)]

        return "port=" + port + ", status=" + status + ", job=" + job + ", keysRead=" + keysRead + ", keysWritten="
                + keysWritten + ", results=" + results;
    }
}