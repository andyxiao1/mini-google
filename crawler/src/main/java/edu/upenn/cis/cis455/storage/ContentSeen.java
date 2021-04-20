package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class ContentSeen {

    @PrimaryKey
    public String md5;

    private ContentSeen() {
    } // For bindings

    public ContentSeen(String hash) {
        md5 = hash;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("ContentSeen[");
        buffer.append("md5=").append(md5).append("]");
        return buffer.toString() + "\n";
    }
}
