package edu.upenn.cis.cis455.xpathengine;

public interface XPathEngine {
    /**
     * Sets the XPath expression(s) that are to be evaluated.
     *
     * @param expressions
     */
    public void setXPaths(String[] expressions);

    /**
     * Event driven pattern match.
     *
     * Takes an event at a time as input
     *
     * @param event notification about something parsed, from a given document
     *
     * @return bit vector of matches to XPaths
     */
    public boolean[] evaluateEvent(OccurrenceEvent event);

}
