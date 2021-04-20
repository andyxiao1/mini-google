package edu.upenn.cis.cis455.xpathengine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DocumentState {

    boolean[] matches;
    Map<String, ElementState> queryIndex;
    int level;
    List<PathNode> filterCheck;

    public DocumentState() {
        level = 0;
        filterCheck = new ArrayList<PathNode>();
    }
}

class ElementState {
    List<PathNode> candidateList;
    List<PathNode> waitList;

    public ElementState() {
        candidateList = new ArrayList<PathNode>();
        waitList = new ArrayList<PathNode>();
    }
}

class PathNode {
    int queryId;
    int level;
    String element;
    List<Filter> filters;
    PathNode nextPathNode;

    public PathNode(int qId, int nodeLevel, String elt, String filter) {
        queryId = qId;
        level = nodeLevel;
        element = elt;
        filters = new ArrayList<Filter>();
        parseFilter(filter);
    }

    public PathNode(PathNode other) {
        queryId = other.queryId;
        level = other.level;
        element = other.element;
        nextPathNode = other.nextPathNode;
        filters = new ArrayList<Filter>();

        for (Filter f : other.filters) {
            filters.add(new Filter(f));
        }
    }

    private void parseFilter(String filter) {
        while (filter.contains("[")) {
            String currFilter = filter.substring(1, filter.indexOf("]", 1));
            filter = filter.substring(filter.indexOf("]", 1) + 1);
            String matchText = "";
            boolean isExactMatch = false;

            if (currFilter.startsWith("text()=\"")) {
                matchText = currFilter.substring(8, currFilter.length() - 1);
                isExactMatch = true;
            } else if (currFilter.startsWith("contains(text(),\"")) {
                matchText = currFilter.substring(17, currFilter.length() - 2);
                isExactMatch = false;
            }
            filters.add(new Filter(matchText, isExactMatch));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PathNode)) {
            return false;
        }
        PathNode node = (PathNode) o;
        return queryId == node.queryId && level == node.level && element.equals(node.element);
    }
}

class Filter {
    boolean isExactMatch;
    String filter;

    public Filter(String matchText, boolean isMatch) {
        filter = matchText.toLowerCase();
        isExactMatch = isMatch;
    }

    public Filter(Filter other) {
        filter = other.filter;
        isExactMatch = other.isExactMatch;
    }

    public boolean isMatch(String text) {
        return isExactMatch ? text.equalsIgnoreCase(filter) : text.toLowerCase().contains(filter);
    }
}