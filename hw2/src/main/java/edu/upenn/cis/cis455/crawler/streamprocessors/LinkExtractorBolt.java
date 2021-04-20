package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
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
import org.jsoup.select.Elements;

public class LinkExtractorBolt implements IRichBolt {

    static final Logger logger = LogManager.getLogger(LinkExtractorBolt.class);

    /**
     * The `LinkExtractorBolt` extracts links from a document and streams the urls
     * as Strings.
     */
    Fields schema = new Fields("url");

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
            CrawlerState.noMoreLinks = true;
            return;
        }

        String document = input.getStringByField("document");
        String contentType = input.getStringByField("contentType");
        boolean isCachedVersion = (boolean) input.getObjectByField("isCachedVersion");

        logger.info(getExecutorId() + " received document for " + url);

        // If not already cached, store document in index.
        if (!isCachedVersion) {
            logger.info(url + ": storing new content");
            database.addDocument(url, document, contentType);
        }

        // Only parse html documents for links.
        if (contentType == null || !contentType.equals(HTML_CONTENT_TYPE)) {
            logger.info(url + ": not parsing because it is not an html file");
            return;
        }

        // Parse for links and emit.
        logger.info(url + ": parsing");
        Document doc = Jsoup.parse(document, url);
        Elements links = doc.getElementsByAttribute("href");
        for (Element link : links) {
            String linkHref = link.attr("abs:href");
            logger.info(getExecutorId() + " emitting new link " + linkHref);
            collector.emit(new Values<Object>(linkHref));
        }
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void setRouter(IStreamRouter router) {
        collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return schema;
    }
}
