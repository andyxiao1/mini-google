package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.DynamoDBInstance;
import edu.upenn.cis.cis455.storage.DynamoFactory;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
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
    Fields schema = new Fields("domain", "url");

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
    DynamoDBInstance dynamoDB;

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
        dynamoDB = DynamoFactory.getDatabaseInstance();
    }

    @Override
    public boolean execute(Tuple input) {
        String url = input.getStringByField("url");

        String document = input.getStringByField("document");
        String contentType = input.getStringByField("contentType");

        logger.debug(getExecutorId() + " received document for " + url);

        // Only parse html documents for links.
        if (contentType == null || !contentType.equals(HTML_CONTENT_TYPE)) {
            logger.debug(url + ": not parsing because it is not an html file");
            return true;
        }

        // Parse for links and emit.
        logger.debug(url + ": parsing");
        Document doc = Jsoup.parse(document, url);
        Elements links = doc.getElementsByAttribute("href");
        for (Element link : links) {
            String linkHref = link.attr("abs:href");
            logger.debug(getExecutorId() + " emitting new link " + linkHref);
            String domain = (new URLInfo(linkHref)).getBaseUrl();
            collector.emit(new Values<Object>(domain, linkHref), getExecutorId());
        }
        return true;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void setRouter(StreamRouter router) {
        collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return schema;
    }
}
