package edu.upenn.cis.cis455.crawler.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.halt;
import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import edu.upenn.cis.cis455.storage.DatabaseEnv;

public class RegistrationHandler implements Route {

    static final Logger logger = LogManager.getLogger(RegistrationHandler.class);
    DatabaseEnv database;

    public RegistrationHandler(DatabaseEnv db) {
        database = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        String user = req.queryParams("username");
        String pass = req.queryParams("password");

        logger.info("Registration request for " + user);

        if (user == null || pass == null) {
            logger.info("Invalid registration");
            halt(401);
        }

        if (database.addUser(user, pass) != -1) {
            logger.info("Registered!");
            return "localhost:45555/login-form.html";
        } else {
            logger.info("Invalid registration");
            halt(401);
        }

        return "";
    }
}
