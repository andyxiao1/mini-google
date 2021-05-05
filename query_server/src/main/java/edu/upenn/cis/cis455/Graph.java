package edu.upenn.cis.cis455;

import java.io.File;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Graph {
	private static final Logger logger = LogManager.getLogger(Graph.class);
	
	private Map<Node, LinkedList<Node>> adj;
	
	private Map<Integer, LinkedList<Node>> idToNeighbors;
	
	private Map<Integer, Integer> idToIndexArr;
	
	private Map<Integer, Node> idToNode;
	
	
	
	private int countNodes;
	
	/**
	 * Update the local variables using the info from this line
	 * @param line
	 */
	public void parseLine(String line) {
		logger.debug(line);
		String[] arr = line.split(",");

		// first check if this Node already exists as a key in our adj

		// if it does, then we grab that node's adj list and update


		// if it doesn't then we create the node, put it in the 
		
		Node base = new Node(arr[0]);
		
		for (int i = 1; i < arr.length; i++) {

			// Node n = new Node(arr[i]);

			// if (adj.containsKey(n)) {
				
			// }

			base.addNeighbor(new Node(arr[i]));
			
		}
		
		adj.put(base, (LinkedList<Node>) base.getOutNeighbors());

	}
	
	public void generateFromFile(File file) {
	    Scanner sc;
		try {
			sc = new Scanner(file);
			sc.useDelimiter(",");
		    while (sc.hasNextLine()) {
			     parseLine(sc.nextLine());
			  }
		} catch (FileNotFoundException e) {
			logger.error("Problem with file");
			e.printStackTrace();
		}
		
	}

	public Graph(File file) {
		
		this.adj = new HashMap<Node, LinkedList<Node>> ();

		this.countNodes = 0;
		
		generateFromFile(file);
		
		logger.info(this.toPrettyString());
	}
    
    /**
     * from node u to node v
     * @param u
     * @param v
     * @return
     */
    public boolean hasEdge(Node u, Node v) {
        if (!adj.containsKey(u) || !adj.containsKey(v)) {
            throw new IllegalArgumentException();
        }
        
        if (u.getOutNeighbors().isEmpty()) {
            return false;
        }
        
        LinkedList<Node> nodes = adj.get(u);
        
        for (Node n : nodes) {
            if (n.getID() == v.getID()) {
                return true;
            }
        }
       
        return false;
    }
    
    public boolean addEdge(Node u, Node v) {
        if (!adj.containsKey(u) || !adj.containsKey(v)) {
            throw new IllegalArgumentException();
        }

        if (hasEdge(u, v)) {
            return false;
        }
        
        u.addNeighbor(v);
        
        LinkedList<Node> l = adj.get(u);
        
        l.add(v);
        adj.replace(u, l);
        
        return true;
    }
    
    
    public LinkedList<Node> getOutNeighbors(Node node) {
        if (node.getID() < 0 || node.getID() >= adj.size()) {
            throw new IllegalArgumentException();
        }
        return adj.get(node);
    }
    
    
    public int outDegree(Node node) {
        return getOutNeighbors(node).size();
    }

	public int getSize() {
		return adj.size();
	}

	public Map<Node, LinkedList<Node>> getMap() {
		return this.adj;
	}
	
	
	public String toPrettyString() {
		StringBuilder sb = new StringBuilder();
		
		for (Node n : adj.keySet()) {
			sb.append("ID: " + n.getIndex() + " ")
			.append("URL: " + n.getURL() + " \n");
			sb.append("Connected to : ");
			for (Node nextNode : n.getOutNeighbors()) {
				sb.append("ID: " + nextNode.getIndex() + " " + "URL: " + nextNode.getURL() + ", ");
			}
			sb.append("\n");
		}
		
		return sb.toString();
		
	}


}
