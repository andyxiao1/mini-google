/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.stormlite;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tasks.ITask;

/**
 * Information about the execution of a topology, including the stream routers
 *
 * @author zives
 *
 */
public class TopologyContext {
    Topology topology;

    FairTaskQueue fairTaskQueue;

    public static enum STATE {
        IDLE, MAPPING, WAITING, REDUCING
    };

    STATE state = STATE.IDLE;

    int mapOutputs = 0;

    int reduceOutputs = 0;

    int keysRead = 0;

    Map<String, Integer> sendOutputs = new HashMap<>();

    /**
     * Mappings from stream IDs to routers
     */
    Map<String, StreamRouter> next = new HashMap<>();

    public TopologyContext(Topology topo, FairTaskQueue theTaskQueue) {
        topology = topo;
        fairTaskQueue = theTaskQueue;
    }

    public Topology getTopology() {
        return topology;
    }

    public void setTopology(Topology topo) {
        this.topology = topo;
    }

    public void addStreamTask(String className, ITask next) {
        fairTaskQueue.addTask(className, next);
    }

    public STATE getState() {
        return state;
    }

    public void setState(STATE state) {
        this.state = state;
    }

    public int getMapOutputs() {
        return mapOutputs;
    }

    public void incMapOutputs(String key) {
        this.mapOutputs++;
    }

    public int getReduceOutputs() {
        return reduceOutputs;
    }

    public void incReduceOutputs(String key) {
        this.reduceOutputs++;
    }

    public void incKeysRead() {
        keysRead++;
    }

    public int getKeysRead() {
        return keysRead;
    }

    public void incSendOutputs(String key) {
        if (!sendOutputs.containsKey(key))
            sendOutputs.put(key, Integer.valueOf(0));

        sendOutputs.put(key, Integer.valueOf(sendOutputs.get(key) + 1));
    }

    public Map<String, Integer> getSendOutputs() {
        return sendOutputs;
    }

}
