package edu.upenn.cis.cis455.crawler.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UrlReader {

    static Logger log = LogManager.getLogger(UrlReader.class);

    public static List<String> readSeedUrls(String filename) {
        List<String> seedUrls = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(filename); // creates a new file instance
            fr = new FileReader(file); // reads the file
            br = new BufferedReader(fr); // creates a buffering character input stream
            String line;
            while ((line = br.readLine()) != null) {
                log.debug("Reading in seed url: " + line.trim());
                seedUrls.add(line.trim());
            }
        } catch (IOException e) {
            log.error("Error with seed file");
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return seedUrls;
    }
}
