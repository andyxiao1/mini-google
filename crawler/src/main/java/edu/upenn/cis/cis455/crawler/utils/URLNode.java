package edu.upenn.cis.cis455.crawler.utils;

public class URLNode {
    boolean isValidated;
    String url;
    long lastModifiedDate;

    public URLNode(String urlStr) {
        isValidated = false;
        url = urlStr;
        lastModifiedDate = -1;
    }

    public void validate() {
        isValidated = true;
    }

    public boolean isValidated() {
        return isValidated;
    }

    public String getUrl() {
        return url;
    }

    public void setLastModifiedDate(long lastModified) {
        lastModifiedDate = lastModified;
    }

    public long getLastModifiedDate() {
        return lastModifiedDate;
    }
}
