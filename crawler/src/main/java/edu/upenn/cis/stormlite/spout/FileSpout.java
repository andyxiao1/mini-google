package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
 *
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public abstract class FileSpout implements IRichSpout {
    static Logger log = LogManager.getLogger(FileSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each instance of the
     * FileSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
     * The collector is the destination for tuples; you "emit" tuples there
     */
    SpoutOutputCollector collector;

    /**
     * This is a simple file reader
     */
    String filename;
    BufferedReader reader;
    Random r = new Random();
    String filepath;

    int inx = 0;
    boolean sentEof = false;

    public FileSpout() {
    }

    public abstract String getFilename();

    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        String storageDir = (String) conf.get("storageDirectory");
        String inputDir = (String) conf.get("inputDirectory");
        filepath = storageDir + inputDir;
        filename = getFilename();

        try {
            log.debug("Starting spout for " + filepath + filename);
            log.debug(getExecutorId() + " opening file reader");

            // Since we dynamically grab the file name from the input directory, it already
            // includes any necessary suffix extensions. This also allows for a more
            // flexible design with respect to matching worker nodes to the correct workerId
            // storage directories.
            reader = new BufferedReader(new FileReader(filepath + filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
        if (reader != null)
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    /**
     * The real work happens here, in incremental fashion. We process and output the
     * next item(s). They get fed to the collector, which routes them to targets
     */
    @Override
    public synchronized boolean nextTuple() {
        if (reader != null && !sentEof) {
            try {
                String line = reader.readLine();
                if (line != null) {
                    log.debug(getExecutorId() + " read from file " + filename + ": " + line);
                    this.collector.emit(new Values<Object>(String.valueOf(inx++), line), getExecutorId());
                } else if (!sentEof) {
                    log.info(getExecutorId() + " finished file " + filename + " and emitting EOS");
                    this.collector.emitEndOfStream(getExecutorId());
                    sentEof = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            Thread.yield();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }

    @Override
    public String getExecutorId() {

        return executorId;
    }

    @Override
    public void setRouter(StreamRouter router) {
        this.collector.setRouter(router);
    }

}
