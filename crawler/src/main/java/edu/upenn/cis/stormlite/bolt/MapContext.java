package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.cis455.mapreduce.Context;

public class MapContext implements Context {

    OutputCollector collector;
    TopologyContext context;

    public MapContext(TopologyContext topoContext, OutputCollector outputCollector) {
        context = topoContext;
        collector = outputCollector;
    }

    @Override
    public void write(String key, String value, String sourceExecutor) {
        collector.write(key, value, sourceExecutor);
        context.incMapOutputs(key);
    }

}
