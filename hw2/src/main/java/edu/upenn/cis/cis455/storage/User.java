package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import edu.upenn.cis.cis455.crawler.utils.Security;

@Entity
public class User {

    @PrimaryKey(sequence = "userId")
    public int id;

    @SecondaryKey(relate = Relationship.ONE_TO_ONE)
    public String username;

    public String passwordHash;

    private User() {
    } // For bindings

    public User(String user, String password) {
        username = user;
        passwordHash = Security.sha256Hash(password);
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("User[");
        buffer.append("id=").append(id).append(",username=").append(username).append(",passwordHash=")
                .append(passwordHash).append("]");
        return buffer.toString() + "\n";
    }
}
