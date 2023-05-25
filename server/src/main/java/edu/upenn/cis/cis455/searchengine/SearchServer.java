package edu.upenn.cis.cis455.searchengine;

import static spark.Spark.*;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SearchServer {
	static Logger log = LogManager.getLogger(SearchServer.class);
	public static String dbDir;
	public static void search() {
		log.info("Creating server listener");
        get("/search", new SearchResultRouter());
        
        after("/search", (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET");
        });
	}

	public static void main(String[] args) {
		if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        port(myPort);
        String dbDir = args[1];
        SearchServer.dbDir = dbDir;
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis", Level.DEBUG);

        System.out.println("Search engine startup, on port " + myPort);

		search();
        awaitInitialization();
        System.out.println("Waiting to handle requests!");
	}

}


