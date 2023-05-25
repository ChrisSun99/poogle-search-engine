package edu.upenn.cis.cis455.storage.berkeleydb;

import java.util.HashMap;
import java.util.Map;

public class StorageFactory {
    private static final Map<String, StorageImpl> INSTANCES = new HashMap<>();

    public static StorageInterface getDatabaseInstance(String directory) {
        if (INSTANCES.containsKey(directory)) {
            return INSTANCES.get(directory);
        } else {
            StorageImpl storage = new StorageImpl(directory);
            INSTANCES.put(directory, storage);
            return storage;
        }
    }
}
