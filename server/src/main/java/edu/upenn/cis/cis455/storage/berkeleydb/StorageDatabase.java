package edu.upenn.cis.cis455.storage.berkeleydb;

import java.io.File;
import java.io.FileNotFoundException;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

class StorageDatabase {

    // Database Environment
    private Environment env;
    // Class Catalog
    private static final String CLASS_CATALOG = "java_class_catalog";
    private StoredClassCatalog javaCatalog;
    // Databases
    private static final String USER_STORE = "user_store";
    private static final String URL_MD5_STORE = "document_url_md5_store";
    private static final String MD5_DOC_STORE = "document_md5_doc_store";
    private Database userDb;
    private Database urlMd5Db;
    private Database md5DocDb;

    public StorageDatabase(String directory) throws DatabaseException, FileNotFoundException {
        // Environment Configuration
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        // Opening the Database Environment
        env = new Environment(new File(directory), envConfig);
        // Database Configuration
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        // Opening the Class Catalog
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);
        // Opening Databases
        userDb = env.openDatabase(null, USER_STORE, dbConfig);
        urlMd5Db = env.openDatabase(null, URL_MD5_STORE, dbConfig);
        md5DocDb = env.openDatabase(null, MD5_DOC_STORE, dbConfig);
    }

    public final Environment getEnvironment() {
        return env;
    }

    public final StoredClassCatalog getClassCatalog() {
        return javaCatalog;
    }

    public final Database getUserDatabase() {
        return userDb;
    }

    public final Database getUrlMd5Database() {
        return urlMd5Db;
    }

    public final Database getMd5DocDatabase() {
        return md5DocDb;
    }

    public void close() throws DatabaseException {
        if (userDb != null) {
            userDb.close();
        }
        if (urlMd5Db != null) {
            urlMd5Db.close();
        }
        if (md5DocDb != null) {
            md5DocDb.close();
        }
        if (javaCatalog != null) {
            javaCatalog.close();
        }
        if (env != null) {
            env.cleanLog();
            env.close();
        }
    }

}
