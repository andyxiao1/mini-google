package edu.upenn.cis.stormlite.storage;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class ReduceState {

    @PrimaryKey
    public String key;

    public List<String> values;

    private ReduceState() {
    } // For bindings

    public ReduceState(String reduceKey) {
        key = reduceKey;
        values = new ArrayList<String>();
    }

    public void addValue(String value) {
        values.add(value);
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("Document[");
        buffer.append("key=").append(key).append(",values=").append(values).append("]");
        return buffer.toString() + "\n";
    }
}
