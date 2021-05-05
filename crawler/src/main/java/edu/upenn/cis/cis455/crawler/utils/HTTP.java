package edu.upenn.cis.cis455.crawler.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

public class HTTP {

    static final Logger logger = LogManager.getLogger(HTTP.class);

    public static String makeRequest(String urlStr, String requestMethod, int maxDocumentSize,
            Map<String, String> responseHeaders) {

        logger.debug("Making request at " + new Timestamp(System.currentTimeMillis()));
        long start = System.currentTimeMillis();

        HttpURLConnection connection = null;
        InputStream responseStream = null;

        if (urlStr == null || urlStr.equals("")) {
            logger.error("url String is empty");
            return null;
        }

        try {
            URLInfo urlInfo = new URLInfo(urlStr);
            URL url = new URL(urlInfo.getProtocol(), urlInfo.getHostName(), urlInfo.getPortNo(), urlInfo.getFilePath());

            // Should also work for `HttpsURLConnection` connections because it is a
            // subclass of `HttpURLConnection` and we have appropriately set the port and
            // protocol
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("User-Agent", "cis455crawler");
            connection.setRequestMethod(requestMethod);
            connection.setConnectTimeout(1200);
            connection.setReadTimeout(1200);
            // HttpURLConnection.setFollowRedirects(false);
            responseStream = connection.getInputStream();

            byte[] response = new byte[maxDocumentSize];
            int read = -1;
            int rlen = 0;

            do {
                read = responseStream.read(response, rlen, maxDocumentSize - rlen);
                rlen += read > -1 ? read : 0; // We do this because the last read may be -1
            } while (rlen < maxDocumentSize && read > 0);

            // Get only the headers we care about for our purposes.
            if (responseHeaders != null) {
                String contentLength = connection.getHeaderField("Content-Length");

                if (contentLength != null) {
                    responseHeaders.put("Content-Length", contentLength.trim());
                }

                String contentType = connection.getHeaderField("Content-Type");
                if (contentType != null) {
                    if (contentType.contains(";")) {
                        contentType = contentType.substring(0, contentType.indexOf(";"));
                    }
                    responseHeaders.put("Content-Type", contentType.trim());
                }

                String lastModified = connection.getHeaderField("Last-Modified");
                if (lastModified != null) {
                    responseHeaders.put("Last-Modified", lastModified.trim());
                }

                long end = System.currentTimeMillis();
                double requestTime = ((double) end) / start / 1000;
                responseHeaders.put("Request-Time", Double.toString(requestTime));
            }

            if (rlen > -1) {
                response = Arrays.copyOf(response, rlen);
            }
            return (new String(response, "UTF-8")).trim();

        } catch (java.net.SocketTimeoutException e) {
            logger.error(urlStr + ": Timed Out at " + new Timestamp(System.currentTimeMillis()));
            logger.debug(e);
        } catch (MalformedURLException e) {
            logger.error(urlStr + ": Error Malformed URL");
            logger.debug(e);
        } catch (UnsupportedEncodingException e) {
            logger.error(urlStr + ": Error converting body byte array to string.");
            logger.debug(e);
        } catch (IOException e) {
            logger.error(urlStr + ": Error fetching document");
            logger.error(e);
        } finally {
            if (responseStream != null) {
                try {
                    responseStream.close();
                } catch (IOException e) {
                    logger.debug("Error closing stream at url: " + urlStr);
                    logger.debug(e);
                }
            }

            if (connection != null) {
                connection.disconnect();
            }
        }

        return null;
    }

    /**
     * Send data with POST request.
     *
     * @throws IOException
     */
    public static int sendData(String urlStr, String requestMethod, String body) throws IOException {
        logger.info("Sending data to " + urlStr);

        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(requestMethod);

        if (requestMethod.equals("POST")) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            OutputStream os = conn.getOutputStream();
            byte[] toSend = body.getBytes();
            os.write(toSend);
            os.flush();
        }

        conn.getInputStream();
        conn.disconnect();

        return conn.getResponseCode();
    }
}
