package edu.upenn.cis.stormlite.bolt;

import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.TopologyContext.STATE;
import edu.upenn.cis.stormlite.distributed.ConsensusTracker;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.cis455.mapreduce.Context;
import edu.upenn.cis.cis455.mapreduce.Job;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "map"
 * on a per-tuple basis.
 *
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

public class MapBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(MapBolt.class);

    Job mapJob;

    /**
     * This object can help determine when we have reached enough votes for EOS
     */
    ConsensusTracker votesForEos;

    /**
     * To make it easier to debug: we have a unique ID for each instance of the
     * WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    Fields schema = new Fields("key", "value");

    boolean sentEos = false;

    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;

    private Context context;

    private TopologyContext topoContext;

    public MapBolt() {
    }

    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        topoContext = context;
        this.context = new MapContext(context, collector);

        if (!stormConf.containsKey("mapClass")) {
            throw new RuntimeException("Mapper class is not specified as a config option");
        } else {
            String mapperClass = stormConf.get("mapClass");

            try {
                mapJob = (Job) Class.forName(mapperClass).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to instantiate the class " + mapperClass);
            }
        }

        if (!stormConf.containsKey("spoutExecutors")) {
            throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        }

        int numSpoutExecutors = Integer.parseInt(stormConf.get("spoutExecutors"));
        int numWorkers = WorkerHelper.getWorkers(stormConf).length;
        int votesNeeded = numSpoutExecutors * numWorkers;
        votesForEos = new ConsensusTracker(votesNeeded);
    }

    /**
     * Process a tuple received from the stream, incrementing our counter and
     * outputting a result
     */
    @Override
    public synchronized boolean execute(Tuple input) {
        if (!input.isEndOfStream()) {
            topoContext.setState(STATE.MAPPING);
            topoContext.incKeysRead();

            String key = input.getStringByField("key");
            String value = input.getStringByField("value");
            log.debug(getExecutorId() + " received " + key + " / " + value + " from executor "
                    + input.getSourceExecutor());

            if (sentEos) {
                throw new RuntimeException("We received data from " + input.getSourceExecutor()
                        + " after we thought the stream had ended!");
            }

            mapJob.map(key, value, context, getExecutorId());

        } else if (input.isEndOfStream()) {
            log.debug("Processing EOS from " + input.getSourceExecutor());

            boolean isEos = votesForEos.voteForEos(input.getSourceExecutor());
            if (isEos) {
                topoContext.setState(STATE.WAITING);
                log.info(getExecutorId() + " finished map and is emitting EOS");
                collector.emitEndOfStream(getExecutorId());
                sentEos = true;
            }
        }
        return !sentEos;
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
    @Override
    public String getExecutorId() {
        return executorId;
    }

    /**
     * Called during topology setup, sets the router to the next bolt
     */
    @Override
    public void setRouter(StreamRouter router) {
        this.collector.setRouter(router);
    }

    /**
     * The fields (schema) of our output stream
     */
    @Override
    public Fields getSchema() {
        return schema;
    }
}
