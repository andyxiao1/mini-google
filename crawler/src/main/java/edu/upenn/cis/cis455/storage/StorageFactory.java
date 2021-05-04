package edu.upenn.cis.cis455.storage;

public class StorageFactory {
    private static DatabaseEnv singleton = null;

    public static DatabaseEnv getDatabaseInstance(String directory) {
        if (singleton == null) {
            singleton = new DatabaseEnv(directory);
        }
        return singleton;
    }
}
