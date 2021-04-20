package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sleepycat.persist.EntityCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.TopologyContext.STATE;
import edu.upenn.cis.stormlite.distributed.ConsensusTracker;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.storage.DatabaseEnv;
import edu.upenn.cis.stormlite.storage.ReduceState;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.cis455.mapreduce.Context;
import edu.upenn.cis.cis455.mapreduce.Job;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
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

public class ReduceBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(ReduceBolt.class);

    Job reduceJob;

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
     * Buffer for state, by key In-Memory Implementation - DEPRACATED
     */
    Map<String, List<String>> stateByKey = new HashMap<>();

    DatabaseEnv database;

    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;

    private Context context;

    private TopologyContext topoContext;

    int neededVotesToComplete = 0;

    public ReduceBolt() {
    }

    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        topoContext = context;
        this.context = new ReduceContext(context, collector);

        if (!stormConf.containsKey("reduceClass"))
            throw new RuntimeException("Reduce class is not specified as a config option");
        else {
            String reduceClass = stormConf.get("reduceClass");

            try {
                reduceJob = (Job) Class.forName(reduceClass).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to instantiate the class " + reduceClass);
            }
        }
        if (!stormConf.containsKey("mapExecutors")) {
            throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

        int numMapExecutors = Integer.parseInt(stormConf.get("mapExecutors"));
        int numWorkers = WorkerHelper.getWorkers(stormConf).length;
        int votesNeeded = numMapExecutors * numWorkers;
        votesForEos = new ConsensusTracker(votesNeeded);

        // Create database folder if it doesn't exist.
        File dbDir = new File(stormConf.get("storageDirectory") + "tmp-database/");
        dbDir.mkdirs();

        database = new DatabaseEnv(stormConf.get("storageDirectory") + "tmp-database/" + getExecutorId());
    }

    /**
     * Process a tuple received from the stream, buffering by key until we hit end
     * of stream
     */
    @Override
    public synchronized boolean execute(Tuple input) {
        if (sentEos) {
            if (!input.isEndOfStream())
                throw new RuntimeException("We received data after we thought the stream had ended!");
            // Already done!
            return false;
        } else if (input.isEndOfStream()) {
            log.debug("Processing EOS from " + input.getSourceExecutor());

            boolean isEos = votesForEos.voteForEos(input.getSourceExecutor());
            if (isEos) {
                log.info(getExecutorId() + " received all tuples and is starting reduce operation");
                topoContext.setState(STATE.REDUCING);

                log.debug("State of berkeley db before running reduce job: " + database);

                EntityCursor<ReduceState> state = database.getStateCursor();
                for (ReduceState s : state) {
                    reduceJob.reduce(s.key, s.values.iterator(), context, getExecutorId());
                }

                state.close();
                database.reset();

                collector.emitEndOfStream(getExecutorId());
                sentEos = true;
                topoContext.setState(STATE.IDLE);
            }
        } else {
            log.debug("Processing " + input.toString() + " from " + input.getSourceExecutor());

            String key = input.getStringByField("key");
            String value = input.getStringByField("value");
            database.addValue(key, value);
        }
        return !sentEos;
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
        if (database != null) {
            database.close();
        }
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
