package edu.upenn.cis.cis455.crawler.utils;

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
    public static final String THREAD_COUNT = "THREAD_COUNT";
    public static final String WORKER_LIST = "workerList";
    public static final String ENVIRONMENT = "ENVIRONMENT";
    public static final String AWS_DOCUMENTS_FOLDER = "AWS_DOCUMENTS_FOLDER";
    public static final String AWS_URLMAP_FOLDER = "AWS_URLMAP_FOLDER";
    public static final String DOCUMENTS_TABLE_NAME = "DOCUMENTS_TABLE_NAME";

    public static final String LOCAL = "LOCAL";
    public static final String AWS = "AWS";

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
}