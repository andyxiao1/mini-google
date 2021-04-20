package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class OutputBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(OutputBolt.class);

    Fields myFields = new Fields();

    /**
     * To make it easier to debug: we have a unique ID for each instance of the
     * OutputBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    BufferedWriter writer;

    @Override
    public void cleanup() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean execute(Tuple input) {
        if (!input.isEndOfStream()) {
            try {
                String key = input.getStringByField("key");
                String value = input.getStringByField("value");
                String output = "(" + key + "," + value + ")";
                log.info(getExecutorId() + " outputting " + output);
                writer.write(output);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                log.error("Error writing output");
                log.error(e);
            }
        }

        return true;
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {

        String filepath = stormConf.get("storageDirectory") + stormConf.get("outputDirectory");
        String filename = filepath + "output.txt";
        log.info("Creating output file at " + filename);

        // Create output directory and file if it doesn't exist
        File file = new File(filepath);
        file.mkdirs();

        try {
            writer = new BufferedWriter(new FileWriter(filename));
        } catch (IOException e) {
            log.error("Error getting output file");
            log.error(e);
        }
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void setRouter(StreamRouter router) {
        // Do nothing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(myFields);
    }

    @Override
    public Fields getSchema() {
        return myFields;
    }
}
