package edu.upenn.cis.cis455;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ejml.data.DMatrix1Row;
import org.ejml.data.DMatrixD1;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.mult.MatrixVectorMult_DDRM;
import org.ejml.data.DMatrix1Row;


public class PageRank {
	
	private static final Logger logger = LogManager.getLogger(PageRank.class);
	
    double[][] original;
    double[][] update;
    Node[] links;
    // need a vector for the page rank of each restaurant
    double[] pageRankV;
    double damping = 0.0;
    DMatrixD1 initialVec;
    DMatrixD1 finalVec;
    
    // number of times to run the multiplication until convergence
    int matrixExp = 4;
    
    
    public PageRank(Graph g) {
        
        pageRankV = new double[g.getSize()];
        links = new Node[g.getSize()];
        // initialize pageRankV to 1 / N for all values
        for(int i = 0; i < pageRankV.length; i++) {
            pageRankV[i] = 1.0 / (double) g.getSize();
        }
        
        //System.out.println(Arrays.toString(pageRankV));
        //System.out.println(((1.0 - damping)/(double) g.getSize()));
        
        // THIS IS A ROW VECTOR
        initialVec = new DMatrixRMaj(pageRankV);
        
        
        original = new double[g.getSize()][g.getSize()];
        update = new double[g.getSize()][g.getSize()];
        links = new Node[g.getSize()]; 
        
        Map<Node, LinkedList<Node>> h = g.getMap();
        
        Set<Entry<Node, LinkedList<Node>>> s = h.entrySet();
        
        for (int i = 0; i < g.getSize(); i++) {
            for (int j = 0; j < g.getSize(); j++) {
                update[i][j] = ((1.0 - damping)/(double) g.getSize());
            }
        }
        
        for (Entry<Node, LinkedList<Node>> e : s) {
            Node n = e.getKey();
            // TODO: shoudl not be using n.getID() to do anything
            
            links[n.getIndex()] = n;  
            LinkedList<Node> l = e.getValue();
            if (l.isEmpty()) {
                original[n.getIndex()][n.getIndex()] = 1.0;
                update[n.getIndex()][n.getIndex()] = (original[n.getIndex()][n.getIndex()] * damping) + ((1.0 - damping)/ (double) g.getSize());
            }
            
            for (Node r : l) {
                //System.out.print("   " + n.getID());
            	// IDK if this is right
                original[n.getIndex()][n.getIndex()] = (1.0 / (double) n.getOutDegree());
                
                // something seems wrong here
                update[n.getIndex()][r.getIndex()] = (original[n.getIndex()][r.getIndex()] * damping) + ((1.0 - damping)/(double) g.getSize());
            }
        }
        
        
        //System.out.println(Arrays.deepToString(original));
        //System.out.println(Arrays.deepToString(update));
        
        DMatrix1Row N = new DMatrixRMaj(update);
        
        for (int i = 0; i < matrixExp; i++) {
            //System.out.println(((DMatrixRMaj) initialVec).toString());
            finalVec = new DMatrixRMaj(1, g.getSize());
            MatrixVectorMult_DDRM.multAddTransA_small(N, initialVec, finalVec);
            initialVec = finalVec;
            //System.out.println(((DMatrixRMaj) initialVec).toString());
        }
        
        logger.info(((DMatrixRMaj) initialVec).getData().toString());
        
        pageRankV = initialVec.getData();
        
    }
    
    
    public void giveNodesRank() {
        for (int i = 0; i < links.length; i++) {
            links[i].rank = pageRankV[i];
        }
    }
    
    public double[] getFinalVectorArray() {
        return initialVec.getData();
    }
    
    public String finalVectorToString() {
        return ((DMatrixRMaj) initialVec).toString();
    }
    
    public double[][] getOriginal() {
        return original;
    }

}