package edu.upenn.cis.cis455.storage;

import java.util.ArrayList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Channel {

    @PrimaryKey
    public String name;

    public String user;

    public String xpath;

    public ArrayList<String> urls;

    private Channel() {
    } // For bindings

    public Channel(String channelName, String userStr, String xpathPattern) {
        name = channelName;
        user = userStr;
        xpath = xpathPattern;
        urls = new ArrayList<String>();
    }

    public void addDocument(String url) {
        urls.add(url);
    }

    public void clearDocuments() {
        urls = new ArrayList<String>();
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("Channel[");
        buffer.append("name=").append(name).append(",user=").append(user).append(",xpath=").append(xpath)
                .append(",urls=").append(urls).append("]");
        return buffer.toString() + "\n";
    }
}
