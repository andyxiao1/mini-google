package edu.upenn.cis.cis455.storage;

import java.time.Instant;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Document {

    @PrimaryKey(sequence = "docId")
    public int id;

    @SecondaryKey(relate = Relationship.ONE_TO_ONE)
    public String url;

    public String content;

    public String contentType;

    public long lastFetchedDate;

    private Document() {
    } // For bindings

    public Document(String documentUrl, String documentContent) {
        url = documentUrl;
        content = documentContent;
    }

    public void setContent(String documentContent) {
        content = documentContent;
    }

    public String getUrl() {
        return url;
    }

    public String getContent() {
        return content;
    }

    public void setContentType(String type) {
        contentType = type;
    }

    public void updateLastFetchedDate() {
        lastFetchedDate = Instant.now().toEpochMilli();
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("Document[");
        buffer.append("id=").append(id).append(",url=").append(url).append(",content=")
                .append(content.substring(0, Integer.min(content.length(), 10))).append("]");
        return buffer.toString() + "\n";
    }
}
