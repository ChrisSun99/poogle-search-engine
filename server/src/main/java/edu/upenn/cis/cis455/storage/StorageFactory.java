package edu.upenn.cis.cis455.storage;

public class StorageFactory {
    
    private static final StorageInterface INSTANCE = new StorageInterface();

    public static StorageInterface getDatabaseInstance() {
        return INSTANCE;
    }

}
