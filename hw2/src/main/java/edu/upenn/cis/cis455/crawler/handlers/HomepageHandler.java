package edu.upenn.cis.cis455.crawler.handlers;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import edu.upenn.cis.cis455.storage.DatabaseEnv;

public class HomepageHandler implements Route {

    static final Logger logger = LogManager.getLogger(HomepageHandler.class);
    DatabaseEnv database;

    public HomepageHandler(DatabaseEnv db) {
        database = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        logger.info("Homepage reached");

        String user = req.attribute("user");
        String htmlStart = "<!DOCTYPE html><html><head><title>Home</title></head><body>";
        String welcomeTag = "<h2>Welcome " + user + "!</h2>";
        String channelListStart = "<ul>";
        String channelListEnd = "</ul>";
        String htmlEnd = "</body></html>";

        String channelListBody = "";
        List<String> channels = database.getAllChannelNames();
        for (String channel : channels) {
            String link = "<a href=\"http://localhost:45555/show?channel=" + channel + "\">Link</a>";
            channelListBody += "<li>" + channel + ": " + link + "</li>";
        }

        String document = htmlStart + welcomeTag + channelListStart + channelListBody + channelListEnd + htmlEnd;
        return document;
    }
}
