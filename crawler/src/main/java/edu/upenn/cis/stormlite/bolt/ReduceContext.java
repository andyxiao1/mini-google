package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.cis455.mapreduce.Context;

public class ReduceContext implements Context {

    OutputCollector collector;
    TopologyContext context;

    public ReduceContext(TopologyContext topoContext, OutputCollector outputCollector) {
        context = topoContext;
        collector = outputCollector;
    }

    @Override
    public void write(String key, String value, String sourceExecutor) {
        collector.write(key, value, sourceExecutor);
        context.incReduceOutputs(key);
    }
}
