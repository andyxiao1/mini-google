package edu.upenn.cis.cis455.crawler.streamprocessors;

import java.net.MalformedURLException;
import java.util.Map;
import java.util.UUID;

import static edu.upenn.cis.cis455.crawler.utils.Constants.*;

import edu.upenn.cis.cis455.storage.AWSFactory;
import edu.upenn.cis.cis455.storage.AWSInstance;
import edu.upenn.cis.cis455.crawler.utils.CrawlerState;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
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
    AWSInstance awsEnv;
    String environment;

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
        database = StorageFactory.getDatabaseInstance(config.get(DATABASE_DIRECTORY));
        awsEnv = AWSFactory.getDatabaseInstance();
        environment = config.get(ENVIRONMENT);
    }

    @Override
    public boolean execute(Tuple input) {
        String urlDomain = input.getStringByField("domain");
        String url = input.getStringByField("url");
        String document = input.getStringByField("document");
        String requestTime = input.getStringByField("requestTime");

        logger.debug(getExecutorId() + " received document for " + url);
        logger.debug(url + ": extracting links");

        Document doc = Jsoup.parse(document, url);
        Elements links = doc.getElementsByAttribute("href");

        if (!isDocumentValid(document, links)) {
            logger.info(url + ": Low quality document.");
            return true;
        }

        // Parse for links and emit.
        for (Element link : links) {
            String linkHref = link.attr("abs:href");
            logger.debug(getExecutorId() + " emitting new link " + linkHref);
            String domain = null;
            try {
                domain = (new URLInfo(linkHref)).getBaseUrl();
            } catch (MalformedURLException e) {
                continue;
            }

            if (environment.equals(AWS)) {
                awsEnv.addUrl(url, domain);
            }

            collector.emit(new Values<Object>(domain, linkHref), getExecutorId());
        }
        logger.debug(url + ": parse end, total links emitted=" + links.size());

        // Store document in appropriate database.
        logger.info(url + ": storing document");
        if (environment.equals(LOCAL)) {
            database.addDocument(url, document);
        } else if (environment.equals(AWS)) {
            String docExcerpt = doc.text().substring(0, Integer.min(300, doc.text().length()));
            String title = doc.title().substring(0, Integer.min(100, doc.title().length()));
            awsEnv.putDocument(url, document, links.size(), requestTime, urlDomain, docExcerpt, title);
        }
        CrawlerState.count.incrementAndGet();

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

    private boolean isDocumentValid(String content, Elements links) {
        if (content.length() < 200) {
            return false;
        }

        if (links.size() == 0) {
            return false;
        }

        return true;
    }
}
