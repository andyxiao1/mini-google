package edu.upenn.cis.cis455.crawler.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Security {

    static final Logger logger = LogManager.getLogger(Security.class);

    public static String md5Hash(String document) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.debug("No algorithm for MD5 hash.");
            logger.debug(e);
            throw new RuntimeException(e);
        }

        byte[] digest = md.digest(document.getBytes());
        BigInteger numDigest = new BigInteger(1, digest);
        String hash = numDigest.toString(16);
        while (hash.length() < 32) {
            hash = "0" + hash;
        }
        return hash;
    }

    public static String sha256Hash(String password) {
        MessageDigest sha256 = null;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            logger.debug("No algorithm for SHA-256 hash.");
            logger.debug(e);
            throw new RuntimeException(e);
        }

        byte[] digest = sha256.digest(password.getBytes());
        BigInteger numDigest = new BigInteger(1, digest);
        String hash = numDigest.toString(16);
        while (hash.length() < 32) {
            hash = "0" + hash;
        }
        return hash;
    }
}
