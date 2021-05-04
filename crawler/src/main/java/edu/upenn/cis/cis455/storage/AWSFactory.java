package edu.upenn.cis.cis455.storage;


public class AWSFactory {
    private static AWSInstance singleton = null;

    public static AWSInstance getDatabaseInstance() {
        if (singleton == null) {
            singleton = new AWSInstance();
        }
        return singleton;
    }
}
