package edu.upenn.cis.cis455.mapreduce;

import spark.Request;
import spark.Response;
import spark.Route;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.DistributedCluster;

public class RunJobRoute implements Route {
	static Logger log = LogManager.getLogger(RunJobRoute.class);
	DistributedCluster cluster;

	public RunJobRoute(DistributedCluster cluster) {
		this.cluster = cluster;
	}

	@Override
	public Object handle(Request request, Response response) throws Exception {
		log.info("Starting job!");

		cluster.startTopology();

		return "Started";
	}

}
