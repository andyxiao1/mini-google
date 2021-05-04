package edu.upenn.cis.cis455.crawler.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlerState {
    public static AtomicInteger count = new AtomicInteger(0);
    public static AtomicBoolean isShutdown = new AtomicBoolean(false);
}