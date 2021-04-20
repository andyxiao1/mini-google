package edu.upenn.cis.cis455.crawler.handlers;

import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.halt;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import edu.upenn.cis.cis455.storage.Channel;
import edu.upenn.cis.cis455.storage.DatabaseEnv;

public class ShowChannelHandler implements Route {

    static final Logger logger = LogManager.getLogger(ShowChannelHandler.class);
    DatabaseEnv database;

    public ShowChannelHandler(DatabaseEnv db) {
        database = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        logger.info("Show channel handler reached");

        String channelName = req.queryParams("channel");
        logger.info("looking for channel " + channelName);
        Channel channel = database.getChannel(channelName);

        if (channel == null) {
            halt(404);
        }

        String htmlStart = "<!DOCTYPE html><html><head><title>" + channelName + "</title></head><body>";
        String headerStart = "<div class=”channelheader”><header>";
        String headerContent = "Channel name: " + channel.name + "<br>" + "created by: " + channel.user;
        String headerEnd = "</header></div>";
        String header = headerStart + headerContent + headerEnd;

        String bodyStart = "<ul>";
        String bodyEnd = "</ul>";

        String htmlEnd = "</body></html>";

        String documentList = "";
        for (String url : channel.urls) {
            String item = "<li>";
            item += "Crawled on: " + convertDate(database.getDocumentLastFetch(url)) + "<br>";
            item += "Location: " + url;
            item += "<div class=\"document\">" + database.getDocument(url) + "</document>";
            item += "</li>";
            documentList += item;
        }

        String document = htmlStart + header + bodyStart + documentList + bodyEnd + htmlEnd;
        return document;
    }

    private String convertDate(long epochMilli) {
        Instant date = Instant.ofEpochMilli(epochMilli);
        String dateString = date.toString();
        return dateString.substring(0, dateString.length() - 5);
    }
}
