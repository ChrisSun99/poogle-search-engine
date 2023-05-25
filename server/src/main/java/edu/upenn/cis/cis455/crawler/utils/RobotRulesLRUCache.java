package edu.upenn.cis.cis455.crawler.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.jsoup.Jsoup;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;

import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

public class RobotRulesLRUCache {

    SimpleRobotRulesParser robotstxtParser;
    LoadingCache<String, SimpleRobotRules> cache;

    public RobotRulesLRUCache () {
        robotstxtParser = new SimpleRobotRulesParser();
        cache = CacheBuilder.newBuilder()
                .maximumSize(64)
                .build(new CacheLoader<String, SimpleRobotRules>() {
                    @Override
                    public SimpleRobotRules load(String url) {
                        try {
                            Response res = Jsoup.connect(url).method(Method.GET).execute();
                            return robotstxtParser.parseContent(url, res.bodyAsBytes(), res.contentType(), "cis455crawler");
                        } catch (Exception e) {
                            return robotstxtParser.parseContent(url, null, null, "cis455crawler");
                        }
                    }
                });
    }

    public SimpleRobotRules get(String url) {
        return cache.getUnchecked(url);
    }
    
}
