package edu.upenn.cis.cis455.storage;


public class DynamoFactory {
    private static DynamoDBInstance singleton = null;

    public static DynamoDBInstance getDatabaseInstance() {
        if (singleton == null) {
            singleton = new DynamoDBInstance();
        }
        return singleton;
    }
}
