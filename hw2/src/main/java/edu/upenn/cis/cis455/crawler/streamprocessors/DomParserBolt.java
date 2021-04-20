package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.xpathengine.OccurrenceEvent;
import edu.upenn.cis.cis455.xpathengine.OccurrenceEvent.Type;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.parser.Parser;

public class DomParserBolt implements IRichBolt {
    static final Logger logger = LogManager.getLogger(DomParserBolt.class);

    /**
     * The `DomParserBolt` streams occurrence events to the path matcher bolt.
     */
    Fields schema = new Fields("url", "event", "isFinished");

    /**
     * To make it easier to debug: we have a unique ID for each instance.
     */
    String executorId = UUID.randomUUID().toString();

    /**
     * This is where we send our output stream.
     */
    private OutputCollector collector;

    /**
     * Interface for database methods.
     */
    DatabaseEnv database;

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    @Override
    public void prepare(Map<String, String> config, TopologyContext context, OutputCollector coll) {
        collector = coll;
        database = (DatabaseEnv) StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");

        if (url == null) {
            return;
        }
        String document = input.getStringByField("document");
        String contentType = input.getStringByField("contentType");
        boolean isHtml = contentType.equals("text/html");

        logger.info(getExecutorId() + " received document for " + url);
        logger.info(url + ": parsing dom occurrence events");

        Document doc = null;
        if (isHtml) {
            doc = Jsoup.parse(document, url);
        } else {
            doc = Jsoup.parse(document, url, Parser.xmlParser());
        }

        domTraversal(doc.child(0), url, isHtml);
        // logger.info(getExecutorId() + " emitting close document event");
        collector.emit(new Values<Object>(url, null, true));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void setRouter(IStreamRouter router) {
        collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return schema;
    }

    private void domTraversal(Element elt, String url, boolean isHtml) {

        // Open Event
        OccurrenceEvent openEvent = new OccurrenceEvent(Type.Open, elt.tagName(), url, isHtml);
        // logger.info(getExecutorId() + " emitting new open event " + elt.tagName());
        collector.emit(new Values<Object>(url, openEvent, false));

        // Handle Text Node Children
        for (TextNode text : elt.textNodes()) {
            OccurrenceEvent textEvent = new OccurrenceEvent(Type.Text, text.text(), url, isHtml);
            // logger.info(getExecutorId() + " emitting new text event " + text.text());
            collector.emit(new Values<Object>(url, textEvent, false));
        }

        // Handle Children
        for (Element child : elt.children()) {
            domTraversal(child, url, isHtml);
        }

        // Close Event
        OccurrenceEvent closeEvent = new OccurrenceEvent(Type.Close, elt.tagName(), url, isHtml);
        // logger.info(getExecutorId() + " emitting new close event " + elt.tagName());
        collector.emit(new Values<Object>(url, closeEvent, false));
    }
}
