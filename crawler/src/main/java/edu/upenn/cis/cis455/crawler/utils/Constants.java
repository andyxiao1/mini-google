package edu.upenn.cis.cis455.crawler.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class Constants {

    private Constants() {
    } // restrict instantiation

    public static final String URL_SPOUT = "URL_SPOUT";
    public static final String DOC_FETCHER_BOLT = "DOC_FETCHER_BOLT";
    public static final String LINK_EXTRACTOR_BOLT = "LINK_EXTRACTOR_BOLT";
    public static final String LINK_FILTER_BOLT = "LINK_FILTER_BOLT";
    public static final String DOM_PARSER_BOLT = "DOM_PARSER_BOLT";
    public static final String PATH_MATCHER_BOLT = "PATH_MATCHER_BOLT";
    public static final String CLUSTER_TOPOLOGY = "CLUSTER_TOPOLOGY";

    public static final String DATABASE_DIRECTORY = "DATABASE_DIRECTORY";
    public static final String MAX_DOCUMENT_SIZE = "MAX_DOCUMENT_SIZE";
    public static final String WORKER_INDEX = "workerIndex";
    public static final String CRAWL_COUNT = "CRAWL_COUNT";
    public static final String WORKER_LIST = "workerList";
    public static final String START_URL = "START_URL";

    public static final int MEGABYTE_BYTES = 1000000; // 1e6 bytes = 1 mb
    public static final int MAX_ROBOTS_FILE_SIZE = 2 * MEGABYTE_BYTES;

    public static final String HEAD_REQUEST = "HEAD";
    public static final String GET_REQUEST = "GET";
    public static final String POST_REQUEST = "POST";
    public static final String ROBOTS_PATH = "/robots.txt";

    public static final String LAST_MODIFIED_HEADER = "Last-Modified";
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String CONTENT_LENGTH_HEADER = "Content-Length";

    public static final String HTML_CONTENT_TYPE = "text/html";

    public static final String[] VALID_FILE_TYPES = new String[] { "text/html", "text/xml", "application/xml" };
    public static final Set<String> VALID_FILE_TYPES_SET = new HashSet<String>(Arrays.asList(VALID_FILE_TYPES));
}