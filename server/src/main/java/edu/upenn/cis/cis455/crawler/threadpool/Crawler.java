package edu.upenn.cis.cis455.crawler.threadpool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import crawlercommons.robots.SimpleRobotRules;
import edu.upenn.cis.cis455.crawler.utils.NonRepeatingURLQueue;
import edu.upenn.cis.cis455.crawler.utils.RobotRulesLRUCache;
import edu.upenn.cis.cis455.crawler.CrawlerInterface;
import edu.upenn.cis.cis455.crawler.utils.DateUtils;
import edu.upenn.cis.cis455.crawler.utils.UrlUtils;
import edu.upenn.cis.cis455.crawler.utils.TikaUtils;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Crawler implements CrawlerInterface {
    static final Logger logger = LogManager.getLogger(Crawler.class);

    static final int NUM_WORKERS = 1;

    private StorageInterface db;

    private int sizeLimit;
    private AtomicInteger count;
    private int countLimit;

    private NonRepeatingURLQueue urlQueue;
    private RobotRulesLRUCache robotRulesCache;

    private ScheduledExecutorService executor;
    private ExecutorService main;

    public Crawler(String startUrl, StorageInterface db, int size, int count) {
        this.db = db;

        sizeLimit = size * (1024 * 1024); // size in bytes
        this.count = new AtomicInteger(0);
        countLimit = count;

        urlQueue = new NonRepeatingURLQueue();
        urlQueue.add(UrlUtils.normalize(startUrl));
        robotRulesCache = new RobotRulesLRUCache();

        executor = Executors.newScheduledThreadPool(NUM_WORKERS);
        main = Executors.newSingleThreadExecutor();

    }

    /**
     * Main thread
     */
    public void start() {
        main.execute(() -> {
            while (!isDone()) {
                try {
                    String url = urlQueue.take();
                    // logger.info("Crawling: " + url);

                    // --------------------------------------------------
                    // check robots.txt
                    SimpleRobotRules robotRules = robotRulesCache.get(UrlUtils.robots(url));

                    // schedule a thread to handle the url, delayed by crawl-delay
                    executor.schedule(() -> {
                        // check isDone
                        if (isDone()) return;
                        // check head
                        try {
                            Response headRes = Jsoup.connect(url)
                                    .method(Method.HEAD)
                                    .userAgent("cis455crawler")
                                    .ignoreContentType(true)
                                    .followRedirects(true)
                                    .execute();
                            // check file size
                            String len = headRes.header("Content-Length");
                            int contentLength = Integer.parseInt(len != null ? len : "0");
                            if (contentLength > sizeLimit) {
                                return;
                            }
                        } catch (IOException e) {
                            logger.error("Error checking head: " + url + " " + e.getMessage());
                        }
                        // get document
                        try {
                            Response getRes = Jsoup.connect(url)
                                    .method(Method.GET)
                                    .userAgent("cis455crawler")
                                    .ignoreContentType(true)
                                    .followRedirects(true)
                                    .maxBodySize(sizeLimit + 1)
                                    .execute();
                            byte[] bodyBytes = getRes.bodyAsBytes();
                            // double check file size
                            if (bodyBytes.length > sizeLimit) {
                                return;
                            }
                            // prepareing document
                            Date date = DateUtils.parseDateString(getRes.header("Date"));
                            String document = TikaUtils.parseContent(new ByteArrayInputStream(bodyBytes), sizeLimit);
                            if (!TikaUtils.detectLanguage(document).equals("en")) {
                                return;
                            }
                            Set<String> outboundLinks = new HashSet<>();
                            // find all links in the document if html
                            if (TikaUtils.isHTML(getRes.contentType())) {
                                Document doc = getRes.parse();
                                for (Element linkElem : doc.select("a[href]")) {
                                    String link = UrlUtils.normalize(linkElem.attr("abs:href"));
                                    if (robotRules.isAllowed(link)) {
                                        outboundLinks.add(link);
                                    } else {
                                        logger.debug("Blocked by robots.txt: " + link);
                                    }
                                }
                                // reduce lock overhead
                                // System.out.println("outboundLinks: " + outboundLinks);
                                urlQueue.addAll(outboundLinks);
                            }
                            // adding to database
                            if (db.addDocument(url, date, document, outboundLinks)) {
                                logger.info(url + " downloading");
                                incCount();
                            } else {
                                logger.info(url + " not modified");
                            }

                        } catch (Exception e) {
                            logger.error("Failed " + url + " " + e.getMessage());
                            // TODO: need to remove url doc from db if it exists
                            // TODO: 429 Too Many Requests, add back to queue with delay
                        }
                    }, robotRules.getCrawlDelay(), TimeUnit.MILLISECONDS);
                    // --------------------------------------------------

                } catch (RejectedExecutionException e) {
                    // logger.error("RejectedExecutionException: " + e.getMessage());
                } catch (InterruptedException ie) {
                    // Preserve interrupt status
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    public void stop() {
        db.close();
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        main.shutdownNow();
        logger.info("Graceful exit, " + count.get() + " pages crawled");
    }

    /**
     * We've indexed another document
     */
    public void incCount() {
        count.incrementAndGet();
    }

    /**
     * Workers can poll this to see if they should exit, ie the crawl is done
     */
    public boolean isDone() {
        // urlQueue empty for 30 seconds or count limit reached
        if (urlQueue.timeout(60000) 
                && ((ScheduledThreadPoolExecutor) executor).getQueue().size() == 0
                && ((ScheduledThreadPoolExecutor) executor).getActiveCount() == 0) {
            logger.info("urlQueue empty for 30 seconds");
            return true;
        }
        if (count.get() >= countLimit) {
            logger.info("count limit reached");
            return true;
        }
        return false;
        // return urlQueue.timeout(30000) || count.get() >= countLimit;
    }

}
