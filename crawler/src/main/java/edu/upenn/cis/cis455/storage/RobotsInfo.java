package edu.upenn.cis.cis455.storage;

import java.time.Instant;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class RobotsInfo {

    static final Logger logger = LogManager.getLogger(RobotsInfo.class);

    @PrimaryKey
    public String domain;

    public ArrayList<String> disallowedPaths;

    public int crawlDelay;

    public long lastAccessedTime;

    private RobotsInfo() {
    }// For bindings

    public RobotsInfo(String hostname, String robotsFile) {
        domain = hostname;
        disallowedPaths = new ArrayList<String>();
        crawlDelay = 0;
        parseRobotsFile(robotsFile);
    }

    public void access() {
        lastAccessedTime = Instant.now().toEpochMilli();
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("Robots[");
        buffer.append("domain=").append(domain).append(",disallowedPaths=").append(disallowedPaths)
                .append(",crawlDelay=").append(crawlDelay).append(",lastAccessedTime=").append(lastAccessedTime)
                .append("]");
        return buffer.toString() + "\n";
    }

    private void parseRobotsFile(String robotsFile) {
        logger.debug("Parsing robots.txt");

        if (robotsFile == null) {
            logger.debug("robots.txt file is null (request error or dne) so we will assume no restrictions");
            return;
        }

        String[] lines = robotsFile.split("\\r?\\n");
        cleanLines(lines);

        int currLine = findFirstBlock(lines, "cis455crawler");
        if (currLine == -1) {
            currLine = findFirstBlock(lines, "*");
        }
        if (currLine == -1) {
            logger.debug("Found no matching user-agents");
            return;
        }
        currLine++;

        while (currLine < lines.length && !lines[currLine].isEmpty()) {
            String line = lines[currLine];
            currLine++;
            String[] parts = line.split(":");
            if (parts.length < 2) {
                continue;
            }
            String field = parts[0];
            String value = parts[1].trim();

            if (field.equals("Disallow")) {
                disallowedPaths.add(value);
            } else if (field.equals("Crawl-delay")) {
                try {
                    crawlDelay = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    continue;
                }
            } else {
                logger.debug("Unsupported rule: " + line);
            }
        }
    }

    private void cleanLines(String[] lines) {
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            // Remove comments
            if (line.contains("#")) {
                line = line.substring(0, line.indexOf("#"));
            }
            line = line.trim();
            lines[i] = line;
        }
    }

    private int findFirstBlock(String[] lines, String userAgent) {
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (line.startsWith("User-agent:")) {
                String[] parts = line.split(":");
                if (parts.length < 2) {
                    continue;
                }
                String parsedAgent = parts[1].trim();
                if (parsedAgent.equals(userAgent)) {
                    return i;
                }
            }
        }
        return -1;
    }
}
