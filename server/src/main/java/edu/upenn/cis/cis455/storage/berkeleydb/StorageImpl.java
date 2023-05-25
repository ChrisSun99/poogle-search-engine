package edu.upenn.cis.cis455.storage.berkeleydb;

import java.io.FileNotFoundException;
import java.util.Map;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.cis455.storage.utils.StringUtils;

class StorageImpl implements StorageInterface {

    StorageDatabase db;
    StorageViews views;

    public StorageImpl(String directory) {
        try {
            db = new StorageDatabase(directory);
            views = new StorageViews(db);
        } catch (DatabaseException dbe) {
            dbe.printStackTrace();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        }
    }

    @Override
    public synchronized int getCorpusSize() {
        // TODO: Auto-generated method stub
        return 0;
    }

    @Override
    public synchronized boolean addDocument(String url, String documentContents) {
        Map<String, String> urlMd5Map = views.getUrlMd5Map();
        Map<String, String> md5DocMap = views.getMd5DocMap();

        String md5 = StringUtils.hash(documentContents, "MD5");
        String oldMd5 = urlMd5Map.getOrDefault(url, "");

        if (md5.equals(oldMd5)) {
            return false;
        }

        urlMd5Map.put(url, md5);
        // remove redundant oldMd5-doc mapping if no url-oldMd5 mapping exists
        if (!views.getUrlMd5ValueSet().contains(oldMd5)) {
            md5DocMap.remove(oldMd5);
        }

        if (!md5DocMap.containsKey(md5)) {
            md5DocMap.put(md5, documentContents);
            return true;
        }

        return false;
    }

    @Override
    public synchronized String getDocument(String url) {
        Map<String, String> urlMd5Map = views.getUrlMd5Map();
        if (urlMd5Map.containsKey(url)) {
            String md5 = urlMd5Map.get(url);
            Map<String, String> md5DocMap = views.getMd5DocMap();
            if (md5DocMap.containsKey(md5)) {
                return md5DocMap.get(md5);
            }
        }
        return null;
    }

    @Override
    public synchronized boolean addUser(String username, String password) {
        Map<String, String> users = views.getUserMap();
        if (!users.containsKey(username)) {
            users.put(username, StringUtils.hash(password, "SHA-256"));
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean getSessionForUser(String username, String password) {
        Map<String, String> users = views.getUserMap();
        if (users.containsKey(username)) {
            String storedPassword = users.get(username);
            return StringUtils.hash(password, "SHA-256").equals(storedPassword);
        }
        return false;
    }

    @Override
    public synchronized void close() {
        try {
            db.close();
        } catch (DatabaseException dbe) {
            dbe.printStackTrace();
        }
    }
    
}
