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
}
