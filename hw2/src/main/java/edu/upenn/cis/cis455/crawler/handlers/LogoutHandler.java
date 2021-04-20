package edu.upenn.cis.cis455.crawler.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import edu.upenn.cis.cis455.storage.DatabaseEnv;

public class LogoutHandler implements Route {

    static final Logger logger = LogManager.getLogger(LogoutHandler.class);
    DatabaseEnv database;

    public LogoutHandler(DatabaseEnv db) {
        database = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {

        logger.info("Logout request for " + req.session().attribute("user"));
        req.session().invalidate();
        resp.redirect("/login-form.html");
        return "";
    }
}
