package edu.upenn.cis.cis455;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.xpathengine.OccurrenceEvent;
import edu.upenn.cis.cis455.xpathengine.XPathEngine;
import edu.upenn.cis.cis455.xpathengine.XPathEngineImpl;
import edu.upenn.cis.cis455.xpathengine.OccurrenceEvent.Type;

import static org.junit.Assert.assertArrayEquals;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestXFilter {

    static final Logger logger = LogManager.getLogger(TestXFilter.class);

    XPathEngine engine;

    // <channel><title>NYT > Sports</title></channel>
    OccurrenceEvent[] simpleXMLStream = new OccurrenceEvent[] {
            new OccurrenceEvent(Type.Open, "channel", "url1", false),
            new OccurrenceEvent(Type.Open, "title", "url1", false),
            new OccurrenceEvent(Type.Text, "NYT > Sports", "url1", false),
            new OccurrenceEvent(Type.Close, "title", "url1", false),
            new OccurrenceEvent(Type.Close, "channel", "url1", false) };

    // <xml><channel><title>title1</title><title><text>hello</text><text2
    // /></title></channel><channel>channel2<text>channel
    // 2 text</text></channel><test /><xml>
    OccurrenceEvent[] xmlStream1 = new OccurrenceEvent[] { new OccurrenceEvent(Type.Open, "xml", "url1", false),
            new OccurrenceEvent(Type.Open, "channel", "url1", false),
            new OccurrenceEvent(Type.Open, "title", "url1", false),
            new OccurrenceEvent(Type.Text, "title1", "url1", false),
            new OccurrenceEvent(Type.Close, "title", "url1", false),
            new OccurrenceEvent(Type.Open, "title", "url1", false),
            new OccurrenceEvent(Type.Open, "text", "url1", false),
            new OccurrenceEvent(Type.Text, "hello", "url1", false),
            new OccurrenceEvent(Type.Close, "text", "url1", false),
            new OccurrenceEvent(Type.Open, "text2", "url1", false),
            new OccurrenceEvent(Type.Close, "text2", "url1", false),
            new OccurrenceEvent(Type.Close, "title", "url1", false),
            new OccurrenceEvent(Type.Close, "channel", "url1", false),
            new OccurrenceEvent(Type.Open, "channel", "url1", false),
            new OccurrenceEvent(Type.Text, "channel2", "url1", false),
            new OccurrenceEvent(Type.Open, "text", "url1", false),
            new OccurrenceEvent(Type.Text, "channel2 text", "url1", false),
            new OccurrenceEvent(Type.Close, "text", "url1", false),
            new OccurrenceEvent(Type.Close, "channel", "url1", false),
            new OccurrenceEvent(Type.Open, "test", "url1", false),
            new OccurrenceEvent(Type.Close, "test", "url1", false),
            new OccurrenceEvent(Type.Close, "xml", "url1", false) };

    // <html><title>NYT > Sports</title></html>
    OccurrenceEvent[] simpleHtmlStream = new OccurrenceEvent[] { new OccurrenceEvent(Type.Open, "html", "url2", true),
            new OccurrenceEvent(Type.Open, "tItLE", "url2", true),
            new OccurrenceEvent(Type.Text, "NYT > Sports", "url2", true),
            new OccurrenceEvent(Type.Close, "TiTle", "url2", true),
            new OccurrenceEvent(Type.Close, "HTML", "url2", true) };

    OccurrenceEvent[] xmlCaseSensitiveStream = new OccurrenceEvent[] {
            new OccurrenceEvent(Type.Open, "channel", "url1", false),
            new OccurrenceEvent(Type.Open, "title", "url1", false),
            new OccurrenceEvent(Type.Text, "NYT > Sports", "url1", false),
            new OccurrenceEvent(Type.Close, "title", "url1", false),
            new OccurrenceEvent(Type.Close, "channel", "url1", false) };

    // <foo>yessir|hello<bar><baz>goodbye</baz></bar></foo>
    OccurrenceEvent[] xmlMultipleFilters = new OccurrenceEvent[] { new OccurrenceEvent(Type.Open, "foo", "url1", false),
            new OccurrenceEvent(Type.Text, "yessir", "url1", false),
            new OccurrenceEvent(Type.Text, "hello", "url1", false),
            new OccurrenceEvent(Type.Open, "bar", "url1", false), new OccurrenceEvent(Type.Open, "baz", "url1", false),
            new OccurrenceEvent(Type.Text, "goodbye", "url1", false),
            new OccurrenceEvent(Type.Close, "baz", "url1", false),
            new OccurrenceEvent(Type.Close, "bar", "url1", false),
            new OccurrenceEvent(Type.Close, "foo", "url1", false),

    };

    @Before
    public void setUp() {
        engine = new XPathEngineImpl();
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);
    }

    @Test
    public void testCanSetXPaths() {
        logger.debug("testCanSetXPaths");
        String[] queries = new String[] { "/channel/title", "/title" };
        engine.setXPaths(queries);
    }

    @Test
    public void testCanEvaluateEvent() {
        logger.debug("testCanEvaluateEvent");
        String[] queries = new String[] { "/channel/title", "/title" };
        engine.setXPaths(queries);
        engine.evaluateEvent(simpleXMLStream[0]);
    }

    @Test
    public void testSimpleXMLPaths() {
        logger.debug("testSimpleXMLPaths");
        String[] queries = new String[] { "/channel/title", "/title" };
        engine.setXPaths(queries);
        boolean[] actual = null;
        for (OccurrenceEvent event : simpleXMLStream) {
            actual = engine.evaluateEvent(event);
        }
        boolean[] expected = new boolean[] { true, false };
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testXML1Paths() {
        logger.debug("testXML1Paths");
        String[] queries = new String[] { "/xml/channel/title", "/xml/title", "/xml/channel/title/text",
                "/xml/channel/text", "/xml/channel/text2", "/xml/test" };
        engine.setXPaths(queries);
        boolean[] actual = null;
        for (OccurrenceEvent event : xmlStream1) {
            actual = engine.evaluateEvent(event);
        }
        boolean[] expected = new boolean[] { true, false, true, true, false, true };
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testSimpleFilter() {
        logger.debug("testSimpleFilter");
        String[] queries = new String[] { "/channel/title[text()=\"NYT > Sports\"]",
                "/channel[text()=\"NYT > Sports\"]", "/channel/title[contains(text(),\"NYT\")]" };
        boolean[] expected = new boolean[] { true, false, true };
        boolean[] actual = null;
        engine.setXPaths(queries);
        for (OccurrenceEvent event : simpleXMLStream) {
            actual = engine.evaluateEvent(event);
        }
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testMultipleFilters1() {
        logger.debug("testMultipleFilters1");
        String[] queries = new String[] { "/foo[contains(text(),\"hello\")]/bar/baz[contains(text(), \"goodbye\")]" };
        boolean[] expected = new boolean[] { true };
        boolean[] actual = null;

        engine.setXPaths(queries);
        for (OccurrenceEvent event : xmlMultipleFilters) {
            actual = engine.evaluateEvent(event);
        }
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testMultipleFilters2() {
        logger.debug("testMultipleFilters2");
        String[] queries = new String[] { "/foo[text()=\"hello\"][text()=\"yessir\"]" };
        boolean[] expected = new boolean[] { true };
        boolean[] actual = null;

        engine.setXPaths(queries);
        for (OccurrenceEvent event : xmlMultipleFilters) {
            actual = engine.evaluateEvent(event);
        }
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testMultipleDocuments() {
        logger.debug("testMultipleDocuments");
        String[] queries = new String[] { "/channel/title", "/title", "/html/title", "/hTMl/tiTLE" };
        engine.setXPaths(queries);

        boolean[] actualHtml = null;
        boolean[] actualXml = null;

        for (int i = 0; i < 5; i++) {
            actualXml = engine.evaluateEvent(simpleXMLStream[i]);
            actualHtml = engine.evaluateEvent(simpleHtmlStream[i]);
        }

        boolean[] expectedXml = new boolean[] { true, false, false, false };
        boolean[] expectedHtml = new boolean[] { false, false, true, true };
        assertArrayEquals(expectedHtml, actualHtml);
        assertArrayEquals(expectedXml, actualXml);
    }

    @Test
    public void testXMLCaseSensitive() {
        logger.debug("testXMLCaseSensitive");
        String[] queries = new String[] { "/channel/title", "/channel/tiTLE", "/title" };
        engine.setXPaths(queries);
        boolean[] actual = null;
        for (OccurrenceEvent event : xmlCaseSensitiveStream) {
            actual = engine.evaluateEvent(event);
        }
        boolean[] expected = new boolean[] { true, false, false };
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testWhitespaceFilter() {
        logger.debug("testWhitespaceFilter");
    }

    @Test
    public void testTextNotCaseSensitiveFilter() {
        logger.debug("testTextNotCaseSensitiveFilter");
    }

    @Test
    public void testHTML() {
        logger.debug("testHTML");
        String[] queries = new String[] { "/html/title", "/hTMl/tiTLE", "/title" };
        engine.setXPaths(queries);
        boolean[] actual = null;
        for (OccurrenceEvent event : simpleHtmlStream) {
            actual = engine.evaluateEvent(event);
        }
        boolean[] expected = new boolean[] { true, true, false };
        assertArrayEquals(expected, actual);
    }
}