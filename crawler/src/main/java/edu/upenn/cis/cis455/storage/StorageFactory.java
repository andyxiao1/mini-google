package edu.upenn.cis.cis455.storage;

public class StorageFactory {
    private static StorageInterface singleton = null;

    public static StorageInterface getDatabaseInstance(String directory) {
        if (singleton == null) {
            singleton = new DatabaseEnv(directory);
        }
        return singleton;
    }
}
