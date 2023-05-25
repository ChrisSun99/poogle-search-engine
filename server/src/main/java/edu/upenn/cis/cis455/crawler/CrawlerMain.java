package edu.upenn.cis.cis455.crawler;

import edu.upenn.cis.cis455.crawler.threadpool.Crawler;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CrawlerMain {
    static final Logger LOGGER = LogManager.getLogger(CrawlerMain.class);

    /**
     * Main program: init database, start crawler, wait for it to notify that it is
     * done, then close.
     */
    public static void main(String args[]) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis", org.apache.logging.log4j.Level.DEBUG);

        if (args.length < 3 || args.length > 5) {
            System.out.println("Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }

        System.out.println("Crawler starting");
        String startUrl = args[0];
        String envPath = args[1];
        Integer size = Integer.valueOf(args[2]);
        Integer count = args.length == 4 ? Integer.valueOf(args[3]) : 100;

        // StorageInterface db = StorageFactory.getDatabaseInstance(envPath);
        StorageInterface db = StorageFactory.getDatabaseInstance();

        CrawlerInterface crawler = new Crawler(startUrl, db, size, count);

        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);
        crawler.start();

        while (!crawler.isDone())
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        // TODO: final shutdown
        crawler.stop();

        System.out.println("Done crawling!");
    }

}
