package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.storage.Channel;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.xpathengine.OccurrenceEvent;
import edu.upenn.cis.cis455.xpathengine.XPathEngine;
import edu.upenn.cis.cis455.xpathengine.XPathEngineFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PathMatcherBolt implements IRichBolt {

    static final Logger logger = LogManager.getLogger(PathMatcherBolt.class);

    /**
     * The `PathMatcherBolt` doesn't stream anything.
     */
    Fields schema = new Fields();

    /**
     * To make it easier to debug: we have a unique ID for each instance.
     */
    String executorId = UUID.randomUUID().toString();

    /**
     * Interface for database methods.
     */
    DatabaseEnv database;

    Map<String, boolean[]> documentMatchMap;
    XPathEngine engine;
    String[] channelNames;
    String[] queries;

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map<String, String> config, TopologyContext context, OutputCollector coll) {
        database = (DatabaseEnv) StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
        engine = XPathEngineFactory.getXPathEngine();

        documentMatchMap = new HashMap<String, boolean[]>();
        List<Channel> channels = database.getAllChannels();
        channelNames = new String[channels.size()];
        queries = new String[channels.size()];

        for (int i = 0; i < channels.size(); i++) {
            Channel channel = channels.get(i);
            channelNames[i] = channel.name;
            queries[i] = channel.xpath;
        }

        engine.setXPaths(queries);
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        OccurrenceEvent event = (OccurrenceEvent) input.getObjectByField("event");
        boolean isFinished = (boolean) input.getObjectByField("isFinished");
        // logger.info(getExecutorId() + " received event for " + url);

        if (!isFinished) {
            boolean[] matches = engine.evaluateEvent(event);
            documentMatchMap.put(url, matches);
            return;
        }

        boolean[] matches = documentMatchMap.get(url);
        for (int i = 0; i < matches.length; i++) {
            if (matches[i]) {
                logger.info(getExecutorId() + " adding " + url + " to " + channelNames);
                database.addDocToChannel(channelNames[i], url);
            }
        }
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void setRouter(IStreamRouter router) {
    }

    @Override
    public Fields getSchema() {
        return schema;
    }
}
