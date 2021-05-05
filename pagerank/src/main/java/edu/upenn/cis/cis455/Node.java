package edu.upenn.cis.cis455;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Node extends Object{
	private static final Logger logger = LogManager.getLogger(Node.class);

	public double rank;
	private int id;
	private int index;
	private String url;
	
	private List<Node> neighbors;
	
	private Set<Node> neighborSet;
	
	
	
	
	public Node(String url) {
		this.id = urlHash(url);
		this.index = urlToIndex(url);
		this.url = url;
		this.neighbors = new LinkedList<Node>();
		this.neighborSet = new HashSet<Node>();
	}
	
	/**
	 * Will get this from Danny
	 * @param url
	 * @return
	 */
	public int urlToIndex(String url) {
		if (url.equals("https://www.cis.upenn.edu/~cis455/")) {
			return 1;
		} else if (url.equals("https://www.facebook.com/")) {
			return 2;
		}
		
		// google.com
		return 0;
	}

	private int urlHash(String url) {
		// TODO for actually hashing  url
		return url.length();
	}
	
	/**
	 * TODO: make sure you check contains or not
	 * @param n node to add
	 */
	public void addNeighbor(Node n) {
		if (!this.neighborSet.contains(n)) {
			this.neighbors.add(n);
			this.neighborSet.add(n);
		} else {
			logger.debug("This node " + n.getURL() + " already added to " + this.getURL());
		}
		
		
	}

	public String getURL() {
		return this.url;
	}
	
	public List<Node> getOutNeighbors() {
		return this.neighbors;
	}

	public int getIndex() {
		return this.index;
	}
	
	public int getID() {
		return this.id;
	}

	public double getOutDegree() {
		return this.neighbors.size();
	}
	
    public boolean equals(Node n) {
        if (n.getID() == this.id) {
            return true;
        }
        return false;
    }
    
    public int compareTo(Node n) {
        return this.id - n.getID();
    }
    
}
