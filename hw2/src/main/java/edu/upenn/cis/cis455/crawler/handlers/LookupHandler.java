package edu.upenn.cis.cis455.crawler.handlers;

import static spark.Spark.halt;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LookupHandler implements Route {

    static final Logger logger = LogManager.getLogger(LookupHandler.class);

    DatabaseEnv database;

    public LookupHandler(DatabaseEnv db) {
        database = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {

        String url = req.queryParams("url");
        logger.info("Lookup request for: " + url);

        if (!database.containsDocument(url)) {
            halt(404);
        }

        resp.type(database.getDocumentType(url));
        return database.getDocument(url);
    }
}
