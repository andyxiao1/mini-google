package edu.upenn.cis.cis455.mapreduce;

/**
 * Context class as per Hadoop MapReduce -- used to write output from the mapper
 * / reducer
 *
 * @author ZacharyIves
 *
 */
public interface Context {

	// Write a key/value pair to the output stream, documenting
	// which source sent it [the sourceExecutor is really for
	// your own testing, and you can set it to a blank string
	// if you prefer]
	void write(String key, String value, String sourceExecutor);
}
