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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolt.BoltDeclarer;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.SenderBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tasks.ITask;
import edu.upenn.cis.stormlite.tasks.SpoutTask;

/**
 * Use multiple threads to simulate a cluster of worker nodes. Hooks to other
 * nodes in a distributed environment.
 *
 * A thread pool (the executor) executes runnable tasks. Each task involves
 * calling a nextTuple() or execute() method in a spout or bolt, then routing
 * its tuple to the router.
 *
 * @author zives
 *
 */
public class DistributedCluster implements Runnable {
	static Logger log = LogManager.getLogger(DistributedCluster.class);

	static AtomicBoolean quit = new AtomicBoolean(false);

	String theTopology;

	Map<String, List<IRichBolt>> boltStreams = new HashMap<>();
	Map<String, List<IRichSpout>> spoutStreams = new HashMap<>();

	Map<String, StreamRouter> streams = new HashMap<>();

	TopologyContext context;

	ObjectMapper mapper = new ObjectMapper();

	ExecutorService executor;

	FairTaskQueue fairTaskQueue = new FairTaskQueue();

	int numThreads;

	/**
	 * Sets up the topology, instantiating objects (executors) on the local machine.
	 * (Needs to be run on each worker in a distributed cluster.)
	 *
	 * Does not start worker threads; that is in startTopology.
	 *
	 *
	 * @param name
	 * @param config
	 * @param topo
	 * @return
	 * @throws ClassNotFoundException
	 */
	public TopologyContext submitTopology(String name, Config config, Topology topo, int threads)
			throws ClassNotFoundException {

		numThreads = threads;
		executor = Executors.newFixedThreadPool(threads);
		theTopology = name;
		context = new TopologyContext(topo, fairTaskQueue);

		boltStreams.clear();
		spoutStreams.clear();
		streams.clear();

		createSpoutInstances(topo, config);

		createBoltInstances(topo, config);

		fairTaskQueue.addClass(SenderBolt.class.getName());

		createRoutes(topo, config);
		scheduleSpouts();

		return context;
	}

	/**
	 * Starts the worker thread to process events, starting with the spouts.
	 */
	public void startTopology() {
		// Put the run method in a background thread
		quit.set(false);
		new Thread(this).start();
	}

	/**
	 * The main executor loop uses Java's ExecutorService to schedule tasks.
	 */
	public void run() {
		for (int i = 0; i < numThreads; i++) {
			executor.execute(() -> {
				while (!quit.get()) {
					Queue<ITask> taskQueue = fairTaskQueue.nextQueue();

					if (taskQueue == null) {
						log.error("Fair task queue empty");
						Thread.yield();
						continue;
					}

					ITask task = taskQueue.poll();
					if (task == null) {
						Thread.yield();
						continue;
					}

					// log.info("Task: " + task.toString());
					// executor.execute(task);
					task.run();
				}
			});
		}
		executor.shutdown();
	}

	/**
	 * Allocate units of work in the task queue, for each spout
	 */
	private void scheduleSpouts() {
		for (String key : spoutStreams.keySet())
			for (IRichSpout spout : spoutStreams.get(key)) {
				String className = spout.getClass().getName();
				// Need this so SpoutTask can add itself back onto the queue.
				Queue<ITask> taskQueue = fairTaskQueue.getQueue(className);
				fairTaskQueue.addTask(className, new SpoutTask(spout, taskQueue));
			}
	}

	/**
	 * For each spout in the topology, create multiple objects (according to the
	 * parallelism)
	 *
	 * @param topo Topology
	 * @throws ClassNotFoundException
	 */
	private void createSpoutInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String key : topo.getSpouts().keySet()) {
			StringIntPair spout = topo.getSpout(key);

			fairTaskQueue.addClass(spout.getLeft());

			spoutStreams.put(key, new ArrayList<IRichSpout>());
			for (int i = 0; i < spout.getRight(); i++)
				try {
					IRichSpout newSpout = (IRichSpout) Class.forName(spout.getLeft()).newInstance();

					SpoutOutputCollector collector = new SpoutOutputCollector(context);

					newSpout.open(config, context, collector);
					spoutStreams.get(key).add(newSpout);
					log.debug("Created a spout executor " + key + "/" + newSpout.getExecutorId() + " of type "
							+ spout.getLeft());
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
		}
	}

	/**
	 * For each bolt in the topology, create multiple objects (according to the
	 * parallelism)
	 *
	 * @param topo Topology
	 * @throws ClassNotFoundException
	 */
	private void createBoltInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String key : topo.getBolts().keySet()) {
			StringIntPair bolt = topo.getBolt(key);

			fairTaskQueue.addClass(bolt.getLeft());

			boltStreams.put(key, new ArrayList<IRichBolt>());
			int localExecutors = bolt.getRight();
			for (int i = 0; i < localExecutors; i++)
				try {
					OutputCollector collector = new OutputCollector(context);

					IRichBolt newBolt = (IRichBolt) Class.forName(bolt.getLeft()).newInstance();
					newBolt.prepare(config, context, collector);
					boltStreams.get(key).add(newBolt);
					log.debug("Created a bolt executor " + key + "/" + newBolt.getExecutorId() + " of type "
							+ bolt.getLeft());
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
		}
	}

	/**
	 * Link the output streams to input streams, ensuring that the right kinds of
	 * grouping + routing are accomplished
	 *
	 * @param topo
	 * @param config
	 */
	private void createRoutes(Topology topo, Config config) {
		// Add destination streams to the appropriate bolts
		for (String stream : topo.getBolts().keySet()) {
			BoltDeclarer decl = topo.getBoltDeclarer(stream);

			StreamRouter router = decl.getRouter();

			streams.put(stream, router);

			int count = boltStreams.get(stream).size();

			// Create a bolt for each remote worker, give it the same # of entries
			// as we had locally so round-robin and partitioning will be consistent
			int workerId = 0;
			for (String worker : WorkerHelper.getWorkers(config)) {

				// Create one sender bolt for each node aside from us!
				if (workerId++ != Integer.valueOf(config.get("workerIndex"))) {
					SenderBolt sender = new SenderBolt(worker, stream);
					sender.prepare(config, context, null);
					for (int i = 0; i < count; i++) {
						router.addRemoteBolt(sender);
						log.debug("Adding a remote route from " + stream + " to " + worker);
					}

					// Create one local executor for each node for us!
				} else {
					for (IRichBolt bolt : boltStreams.get(stream)) {
						router.addBolt(bolt);
						log.debug("Adding a route from " + decl.getStream() + " to " + bolt);
					}
				}
			}

			if (topo.getBolts().containsKey(decl.getStream())) {
				for (IRichBolt bolt : boltStreams.get(decl.getStream())) {
					bolt.setRouter(router);
					bolt.declareOutputFields(router);
				}
			} else {
				for (IRichSpout spout : spoutStreams.get(decl.getStream())) {
					spout.setRouter(router);
					spout.declareOutputFields(router);
				}
			}

		}
	}

	/**
	 * For each bolt in the topology, clean up objects
	 *
	 * @param topo Topology
	 */
	private void closeBoltInstances() {
		for (List<IRichBolt> boltSet : boltStreams.values())
			for (IRichBolt bolt : boltSet)
				bolt.cleanup();
	}

	/**
	 * For each spout in the topology, create multiple objects (according to the
	 * parallelism)
	 *
	 * @param topo Topology
	 */
	private void closeSpoutInstances() {
		for (List<IRichSpout> spoutSet : spoutStreams.values())
			for (IRichSpout spout : spoutSet)
				spout.close();
	}

	/**
	 * Shut down the cluster
	 *
	 * @param string
	 */
	public void killTopology(String string) {
		if (quit.getAndSet(true) == false) {
			while (!quit.get())
				Thread.yield();
		}
	}

	/**
	 * Shut down the bolts and spouts
	 */
	public void shutdown() {
		closeSpoutInstances();
		closeBoltInstances();

		log.info("Shutting down distributed cluster.");
	}

	public StreamRouter getStreamRouter(String stream) {
		return streams.get(stream);
	}

	public void awaitTermination() {
		try {
			executor.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error("Forced termination");
			// e.printStackTrace();
		}
	}
}
