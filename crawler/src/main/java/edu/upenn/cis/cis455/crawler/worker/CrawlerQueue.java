package edu.upenn.cis.cis455.crawler.worker;

import java.net.MalformedURLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.storage.RobotsInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CrawlerQueue {

    static final Logger logger = LogManager.getLogger(CrawlerQueue.class);
    private static CrawlerQueue singleton = null;

    Queue<UrlQueue> domainQueue;
    Map<String, UrlQueue> domainMap;

    /**
     * We have this because we want access times to persist even if `UrlQueue`s get
     * removed for being empty.
     */
    Map<String, Long> lastAccessedTimeMap;

    /**
     * Internal queue to keep track of the next url for a specific domain. It also
     * keeps a copy of the robots.txt info and last access time for the associated
     * domain.
     */
    class UrlQueue extends LinkedList<String> {

        private static final long serialVersionUID = 6424472732971714838L;
        String domain;
        RobotsInfo robotsInfo;

        public UrlQueue(String hostname) {
            domain = hostname;
        }

        public String toString() {
            return domain + "=" + size();
        }
    }

    public static CrawlerQueue getSingleton() {
        if (singleton == null) {
            singleton = new CrawlerQueue();
        }
        return singleton;
    }

    private CrawlerQueue() {
        domainQueue = new LinkedList<UrlQueue>();
        domainMap = new HashMap<String, UrlQueue>();
        lastAccessedTimeMap = new HashMap<String, Long>();
    }

    ///////////////////////////////////////////////////
    // Domain Specific Methods
    ///////////////////////////////////////////////////

    /**
     * Returns the next domain in the queue
     */
    public String getNextDomain() {
        if (isEmpty()) {
            return null;
        }

        return domainQueue.peek().domain;
    }

    /**
     * Skip to next domain, most likely because the current domain (at the head of
     * the domainQueue) needs to wait due to politeness reasons
     */
    public void skipDomain() {
        if (isEmpty()) {
            return;
        }

        domainQueue.add(domainQueue.remove());
    }

    /**
     * Update the `lastAccessedTime` for a domain
     */
    public void accessDomain(String domain) {
        lastAccessedTimeMap.put(domain, Instant.now().toEpochMilli());
    }

    /**
     * Return the `lastAccessedTime` for a domain
     */
    public long getLastAccessedTime(String domain) {
        if (!lastAccessedTimeMap.containsKey(domain)) {
            return -1;
        }
        return lastAccessedTimeMap.get(domain);
    }

    /**
     * Sets the robots info for a domain
     */
    public void setRobotsInfo(String domain, RobotsInfo robots) {
        if (!domainMap.containsKey(domain)) {
            return;
        }
        domainMap.get(domain).robotsInfo = robots;
    }

    /**
     * Gets the robots info for a domain
     */
    public RobotsInfo getRobotsInfo(String domain) {
        if (!domainMap.containsKey(domain)) {
            return null;
        }
        return domainMap.get(domain).robotsInfo;
    }

    /**
     * Returns true if the domain has robots info set
     */
    public boolean hasRobotsInfo(String domain) {
        return getRobotsInfo(domain) != null;
    }

    ///////////////////////////////////////////////////
    // URL Methods
    ///////////////////////////////////////////////////

    /**
     * Add a new url to the CrawlerQueue.
     */
    public void addUrl(String url) {
        String domain = null;
        try {
            domain = (new URLInfo(url)).getBaseUrl();
        } catch (MalformedURLException e) {
            return;
        }

        if (!domainMap.containsKey(domain)) {
            UrlQueue urlQueue = new UrlQueue(domain);
            domainQueue.add(urlQueue);
            domainMap.put(domain, urlQueue);
        }
        domainMap.get(domain).add(url);
    }

    /**
     * Returns the next url to parse. Worker thread must check politeness
     * constraints and re-add if appropriate. Will remove empty url queues when
     * encountered. Returns null if empty.
     */
    public String removeUrl() {
        if (isEmpty()) {
            return null;
        }

        UrlQueue urlQueue = domainQueue.remove();
        String url = urlQueue.remove();

        // We want to maintain that every urlQueue in the domainQueue is non-empty, so
        // we only add it back if it isn't empty and if it is we remove it from the
        // `domainMap`.
        if (!urlQueue.isEmpty()) {
            domainQueue.add(urlQueue);
        } else {
            domainMap.remove(urlQueue.domain);
        }

        return url;
    }

    public boolean isEmpty() {
        return domainQueue.isEmpty();
    }

    public String toString() {
        // String res = "CRAWLER QUEUE:[";
        int size = 0;
        for (String key : domainMap.keySet()) {
            // res += domainMap.get(key).toString() + ",";
            size += domainMap.get(key).size();
        }
        // res = res.substring(0, res.length() - 1) + "]\n";
        // res += "size=" + size;
        // return res;
        return "size=" + size;
    }
}
