package edu.upenn.cis.cis455.storage;

import java.util.LinkedList;
import java.util.Queue;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class DomainQueue {

    @PrimaryKey
    public String domain;

    public Queue<String> urls;

    private DomainQueue() {
    }// For bindings

    public DomainQueue(String domainName) {
        domain = domainName;
        urls = new LinkedList<String>();
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("DomainQueue[");
        buffer.append("domain=").append(domain).append(",urls=")
                .append(urls.toString().substring(0, Integer.min(urls.toString().length(), 40))).append("]");
        return buffer.toString() + "\n";
    }

    public int size() {
        return urls.size();
    }
}
