package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class CrawlQueue {

    public static long frontId = 0;
    public static long endId = -1;

    @PrimaryKey
    public long id;

    @SecondaryKey(relate = Relationship.ONE_TO_ONE)
    public String domain;

    private CrawlQueue() {
    }// For bindings

    public CrawlQueue(String domainName, boolean addToFront) {
        domain = domainName;
        if (addToFront) {
            moveToFront();
        } else {
            moveToEnd();
        }
    }

    public void moveToFront() {
        // NOTE: id should be invalid at this point ie only call this when this domain
        // isn't currently in the CrawlQueue.
        id = --frontId;
    }

    public void moveToEnd() {
        id = ++endId;
    }

    public void removeFromFront() {
        frontId++;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("DomainQueue[");
        buffer.append("id=").append(id).append(",domain=").append(domain).append("]");
        return buffer.toString() + "\n";
    }

    public static boolean isEmpty() {
        return endId - frontId + 1 == 0;
    }
}
