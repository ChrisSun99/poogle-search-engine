package edu.upenn.cis.cis455.storage.berkeleydb;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

/**
 * BerkeleyDB connector
 *
 */
public class DBWrapper {
	private static Environment env; 
	private static EntityStore store;
	
	public static void openDBWrapper(String path) throws Exception {
		File dbFile = new File(path);
		if (!dbFile.isDirectory()) {
			if (!dbFile.mkdirs()) {
				throw new Exception("DB File make dir failed");
			}
		}
		EnvironmentConfig envConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();
		envConfig.setAllowCreate(true);
		storeConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		storeConfig.setTransactional(true);
		env = new Environment(dbFile, envConfig);
		store = new EntityStore(env, "Crawler Store", storeConfig);
	}
	
	public static void closeDB() throws DatabaseException {
		if (store != null) {
			store.close();
		}
		if (env != null) {
			env.close();
		}
	}
	
	public static EntityStore getStore() {
		return store;
	}
}
