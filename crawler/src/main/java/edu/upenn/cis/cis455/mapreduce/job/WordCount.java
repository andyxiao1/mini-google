package edu.upenn.cis.cis455.mapreduce.job;

import java.util.Iterator;

import edu.upenn.cis.cis455.mapreduce.Context;
import edu.upenn.cis.cis455.mapreduce.Job;

public class WordCount implements Job {

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     *
     */
    public void map(String key, String value, Context context, String sourceExecutor) {
        String[] toks = value.split("\\s+");
        for (String word : toks) {
            context.write(word, "1", "");
        }
    }

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     *
     */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {
        int sum = 0;
        while (values.hasNext()) {
            sum += Integer.parseInt(values.next());
        }
        context.write(key, "" + sum, "");
    }

}
