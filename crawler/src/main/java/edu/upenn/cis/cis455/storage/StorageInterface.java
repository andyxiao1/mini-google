package edu.upenn.cis.cis455.storage;

public interface StorageInterface {

    /**
     * How many documents so far?
     */
    public int getCorpusSize();

    /**
     * Add a new document, getting its ID
     */
    public int addDocument(String url, String documentContents);

    /**
     * Retrieves a document's contents by URL
     */
    public String getDocument(String url);

    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close();
}
