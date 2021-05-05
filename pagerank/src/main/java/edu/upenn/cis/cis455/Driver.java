package edu.upenn.cis.cis455;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Driver {
	private static final Logger logger = LogManager.getLogger(Driver.class);
	
	
	public static void printAllFiles(boolean testing) {
		if (testing) {
			File test = new File(".");
			
	        for (String p : test.list()) {
	            System.out.println(p);
	        }
		}
	}
	
	public static void main(String[] args) {
		
		org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);
		
		logger.info("Driver called");
		
		String pathToGraphTxt = "TestGraphUrls.txt";
		
		boolean testing = false;
		
		printAllFiles(testing);
		
		File f = new File(pathToGraphTxt);
		
		Graph g = new Graph(f);
		
		
		PageRank pg = new PageRank(g);
		
		logger.info("Final : " + pg.finalVectorToString());
		
	}

}
