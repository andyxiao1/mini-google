package edu.upenn.cis.cis455.mapreduce.job;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.upenn.cis.cis455.mapreduce.Context;
import edu.upenn.cis.cis455.mapreduce.Job;

public class GrepJob implements Job {

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     *
     */
    public void map(String key, String value, Context context, String sourceExecutor) {
        Pattern pattern = Pattern.compile("look for this pattern", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(value);
        boolean matchFound = matcher.find();
        if (matchFound) {
            context.write(key, value, "");
        }
    }

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     *
     */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {
        while (values.hasNext()) {
            context.write(key, values.next(), sourceExecutor);
        }
    }
}
