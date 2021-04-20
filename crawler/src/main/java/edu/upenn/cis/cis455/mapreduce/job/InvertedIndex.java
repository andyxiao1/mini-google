package edu.upenn.cis.cis455.mapreduce.job;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.upenn.cis.cis455.mapreduce.Context;
import edu.upenn.cis.cis455.mapreduce.Job;

public class InvertedIndex implements Job {
    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     *
     */
    public void map(String key, String value, Context context, String sourceExecutor) {
        String[] toks = value.split(" ");
        for (int i = 1; i < toks.length; i++) {
            context.write(toks[i], toks[0], "");
        }
    }

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     *
     */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {
        Set<String> unique = new HashSet<String>();
        while (values.hasNext()) {
            unique.add(values.next());
        }
        context.write(key, unique.toString(), "");
    }
}
