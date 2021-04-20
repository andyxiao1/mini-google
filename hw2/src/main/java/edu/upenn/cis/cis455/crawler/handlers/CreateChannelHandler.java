package edu.upenn.cis.cis455.crawler.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.halt;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import edu.upenn.cis.cis455.storage.DatabaseEnv;

public class CreateChannelHandler implements Route {

    static final Logger logger = LogManager.getLogger(CreateChannelHandler.class);

    DatabaseEnv database;
    String newXPath;

    public CreateChannelHandler(DatabaseEnv db) {
        database = db;
        newXPath = "";
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        logger.info("create channel handler reached");

        String user = req.attribute("user");
        String name = req.params("name");
        String xpath = req.queryParams("xpath");

        if (name == null || name.isEmpty() || user == null || user.isEmpty() || xpath == null || xpath.isEmpty()
                || database.containsChannel(name) || !isValidXPath(xpath)) {
            logger.info("Invalid params for creating channel");
            halt(400);
        }

        database.addChannel(name, user, newXPath);

        return "Channel created!";
    }

    private boolean isValidXPath(String xpath) {
        logger.debug("old xpath: " + xpath);
        if (xpath.isEmpty() || xpath.charAt(0) != '/') {
            logger.debug(xpath + ": Invalid xpath (empty or no /)");
            return false;
        }

        String[] paths = xpath.substring(1).split("/");
        newXPath = "";

        // Check that each path node component is valid.
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            String filter = "";
            String newFilter = "";

            if (path.isEmpty()) {
                logger.debug(xpath + ": Invalid xpath empty path node");
                return false;
            }

            if (path.contains("[")) {
                int idx = path.indexOf("[");
                filter = path.substring(idx);
                path = path.substring(0, idx);
            }

            boolean pathContainsWhitespace = path.matches(".*\\s+.*");
            if (pathContainsWhitespace) {
                logger.debug(xpath + ": path contains whitespace");
                return false;
            }

            while (!filter.isEmpty() && filter.contains("\"")) {
                int idx = filter.indexOf("\"");
                newFilter += filter.substring(0, idx + 1).replaceAll("\\s", "");
                filter = filter.substring(idx + 1);

                if (!filter.contains("\"")) {
                    logger.debug(xpath + ": no matching quotes");
                    return false;
                }

                idx = filter.indexOf("\"");
                newFilter += filter.substring(0, idx + 1);
                filter = filter.substring(idx + 1);
            }
            newFilter += filter.replaceAll("\\s", "");
            logger.debug(xpath + ": new filter " + newFilter);
            filter = newFilter;

            while (!filter.isEmpty() && filter.charAt(0) == '[') {
                if (!filter.contains("]")) {
                    logger.debug(xpath + ": no matching brackets");
                    return false;
                }

                String currFilter = filter.substring(1, filter.indexOf("]", 1));
                filter = filter.substring(filter.indexOf("]", 1) + 1);

                boolean valid = currFilter.startsWith("text()=\"") && currFilter.endsWith("\"")
                        || currFilter.startsWith("contains(text(),\"") && currFilter.endsWith("\")");
                if (!valid) {
                    logger.debug(xpath + ": bad predicate");
                    return false;
                }
            }

            newXPath += "/" + path + newFilter;
        }
        logger.debug("new xpath: " + newXPath);
        return true;
    }
}