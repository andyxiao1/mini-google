package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class UrlSeen {

    @PrimaryKey
    public String url;

    private UrlSeen() {
    }// For bindings

    public UrlSeen(String urlStr) {
        url = urlStr;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("UrlSeen[");
        buffer.append("url=").append(url).append("]");
        return buffer.toString() + "\n";
    }
}
