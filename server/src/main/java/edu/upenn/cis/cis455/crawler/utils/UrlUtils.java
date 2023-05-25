package edu.upenn.cis.cis455.crawler.utils;

import javax.annotation.Nullable;

import crawlercommons.filters.basic.BasicURLNormalizer;

public class UrlUtils {

    static final BasicURLNormalizer basicURLNormalizer = new BasicURLNormalizer();

    @Nullable
    public static String normalize(String url) {
        return basicURLNormalizer.filter(url);
    }
    
    @Nullable
    public static String robots(String url) {
        String normalizedURL = basicURLNormalizer.filter(url);
        return normalizedURL != null ? normalizedURL.replaceFirst("/[^/]*$", "/robots.txt") : null;
    }

    public static void main(String[] args) {
        System.out.println(normalize("www.google.com"));
    }

}
