package edu.upenn.cis.cis455.storage;

public class StorageLocks {
    public final static Object CRAWLER_LOCK = new Object();
    public final static Object CRAWLER_CACHE_LOCK = new Object();
    public final static Object URL_SEEN_LOCK = new Object();
    public final static Object URL_SEEN_CACHE_LOCK = new Object();
}
