package edu.upenn.cis.cis455.xpathengine;

import org.xml.sax.helpers.DefaultHandler;

/**
 * Implement this factory to produce your XPath engine and SAX handler as
 * necessary. It may be called by the test/grading infrastructure.
 *
 * @author cis455
 *
 */
public class XPathEngineFactory {
    public static XPathEngine getXPathEngine() {
        return new XPathEngineImpl();
    }

    public static DefaultHandler getSAXHandler() {
        return null;
    }
}
