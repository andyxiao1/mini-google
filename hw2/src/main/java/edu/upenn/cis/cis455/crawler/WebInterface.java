package edu.upenn.cis.cis455.crawler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static spark.Spark.*;

import edu.upenn.cis.cis455.crawler.handlers.CreateChannelHandler;
import edu.upenn.cis.cis455.crawler.handlers.HomepageHandler;
import edu.upenn.cis.cis455.crawler.handlers.LoginFilter;
import edu.upenn.cis.cis455.storage.DatabaseEnv;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.crawler.handlers.LoginHandler;
import edu.upenn.cis.cis455.crawler.handlers.LogoutHandler;
import edu.upenn.cis.cis455.crawler.handlers.LookupHandler;
import edu.upenn.cis.cis455.crawler.handlers.RegistrationHandler;
import edu.upenn.cis.cis455.crawler.handlers.ShowChannelHandler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WebInterface {

    static final Logger logger = LogManager.getLogger(WebInterface.class);

    public static void main(String args[]) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.ALL);

        if (args.length < 1 || args.length > 2) {
            System.out.println("Syntax: WebInterface {path} {root}");
            System.exit(1);
        }

        if (!Files.exists(Paths.get(args[0]))) {
            try {
                Files.createDirectory(Paths.get(args[0]));
            } catch (IOException e) {
                logger.debug("Can't create database environment folder");
                logger.debug(e);
            }
        }

        port(45555);
        DatabaseEnv database = (DatabaseEnv) StorageFactory.getDatabaseInstance(args[0]);

        if (args.length == 2) {
            staticFiles.externalLocation(args[1]);
            staticFileLocation(args[1]);
        }

        before("/*", "*/*", new LoginFilter(database));

        post("/register", new RegistrationHandler(database));
        post("/login", new LoginHandler(database));
        post("/logout", new LogoutHandler(database));
        get("/logout", new LogoutHandler(database)); // For testing purposes.

        get("/create/:name", new CreateChannelHandler(database));
        get("/show", new ShowChannelHandler(database));
        // TODO: get("/create", new CreateChannelHandler(database));

        get("/", new HomepageHandler(database));
        get("/index.html", new HomepageHandler(database));

        // Crawler testing
        get("/lookup", new LookupHandler(database));

        awaitInitialization();
    }
}
