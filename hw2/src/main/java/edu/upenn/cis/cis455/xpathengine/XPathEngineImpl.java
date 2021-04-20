package edu.upenn.cis.cis455.xpathengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class XPathEngineImpl implements XPathEngine {

    static final Logger logger = LogManager.getLogger(XPathEngineImpl.class);

    Map<String, DocumentState> documentStateMap;
    DocumentState context;
    String[] queries;

    public XPathEngineImpl() {
        documentStateMap = new HashMap<String, DocumentState>();
    }

    @Override
    public void setXPaths(String[] expressions) {
        queries = expressions;
    }

    @Override
    public boolean[] evaluateEvent(OccurrenceEvent event) {
        String url = event.getUrl();
        if (!documentStateMap.containsKey(url)) {
            buildFSM(url, event.getIsHtml());
        }
        context = documentStateMap.get(url);

        switch (event.getType()) {
        case Open:
            handleOpen(event);
            break;
        case Close:
            handleClose(event);
            break;
        case Text:
            handleText(event);
            break;
        }
        return context.matches;
    }

    ///////////////////////////////////////////////////
    // Helper Methods
    ///////////////////////////////////////////////////

    private void buildFSM(String url, boolean isHtml) {
        context = new DocumentState();
        documentStateMap.put(url, context);

        context.matches = new boolean[queries.length];
        context.queryIndex = new HashMap<String, ElementState>();

        for (int i = 0; i < queries.length; i++) {
            parseQuery(queries[i], i, isHtml);
        }
    }

    private void parseQuery(String query, int qId, boolean isHtml) {
        if (query.isEmpty() || query.charAt(0) != '/') {
            logger.debug(query + ": Invalid query (empty or no /)");
            return;
        }

        String[] paths = query.substring(1).split("/");
        int numPaths = paths.length;
        PathNode[] pathNodes = new PathNode[numPaths];

        // Parse each path node component.
        for (int i = 0; i < numPaths; i++) {
            String path = paths[i];
            String filter = "";

            if (isHtml) {
                path = path.toLowerCase();
            }

            if (path.isEmpty()) {
                logger.debug(query + ": Invalid query empty path node");
                return;
            }

            if (path.contains("[")) {
                int idx = path.indexOf("[");
                filter = path.substring(idx);
                path = path.substring(0, idx);
            }

            PathNode pathNode = new PathNode(qId, i, path, filter);
            pathNodes[i] = pathNode;
        }

        // Add path nodes to actual data structure and connect nodes to next path node.
        for (int i = 0; i < numPaths; i++) {
            PathNode pathNode = pathNodes[i];
            String element = pathNode.element;

            if (!context.queryIndex.containsKey(element)) {
                context.queryIndex.put(element, new ElementState());
            }

            if (i == 0) {
                context.queryIndex.get(element).candidateList.add(pathNode);
            } else {
                context.queryIndex.get(element).waitList.add(pathNode);
                PathNode prevPathNode = pathNodes[i - 1];
                prevPathNode.nextPathNode = pathNode;
            }
        }
    }

    private void handleOpen(OccurrenceEvent event) {
        logger.debug("handling open: " + event);
        String element = event.getValue();
        context.filterCheck.clear();
        if (context.queryIndex.containsKey(element)) {
            ElementState state = context.queryIndex.get(element);
            for (PathNode pathNode : state.candidateList) {
                if (pathNode.level != context.level) {
                    continue;
                }

                // Move to next state if no filters.
                if (pathNode.filters.isEmpty()) {
                    // Final state case.
                    if (pathNode.nextPathNode == null) {
                        context.matches[pathNode.queryId] = true;
                        continue;
                    }

                    PathNode nextPathNode = pathNode.nextPathNode;
                    ElementState nextEltState = context.queryIndex.get(nextPathNode.element);
                    nextEltState.candidateList.add(new PathNode(nextPathNode));
                } else {
                    // Otherwise, put into an intermediary stage to check filter status.
                    context.filterCheck.add(new PathNode(pathNode));
                }

            }
        }
        context.level++;
    }

    private void handleClose(OccurrenceEvent event) {
        logger.debug("handling close: " + event);
        context.filterCheck.clear();
        context.level--;

        if (context.level == 0) {
            logger.debug("removing document: " + event.getUrl());
            documentStateMap.remove(event.getUrl());
            return;
        }

        String element = event.getValue();
        if (context.queryIndex.containsKey(element)) {
            ElementState state = context.queryIndex.get(element);
            for (PathNode pathNode : state.candidateList) {
                if (pathNode.level != context.level || pathNode.nextPathNode == null) {
                    continue;
                }
                PathNode nextPathNode = pathNode.nextPathNode;
                ElementState nextEltState = context.queryIndex.get(nextPathNode.element);
                nextEltState.candidateList.remove(nextPathNode);
            }
        }
    }

    private void handleText(OccurrenceEvent event) {
        logger.debug("handling text: " + event);

        List<PathNode> remainingNodes = new ArrayList<PathNode>();
        for (PathNode pathNode : context.filterCheck) {
            List<Filter> remainingFilters = new ArrayList<Filter>();
            for (Filter filter : pathNode.filters) {
                if (!filter.isMatch(event.getValue())) {
                    remainingFilters.add(filter);
                }
            }
            pathNode.filters = remainingFilters;

            // Move to next state if no filters.
            if (pathNode.filters.isEmpty()) {
                // Final state case.
                if (pathNode.nextPathNode == null) {
                    context.matches[pathNode.queryId] = true;
                } else {
                    PathNode nextPathNode = pathNode.nextPathNode;
                    ElementState nextEltState = context.queryIndex.get(nextPathNode.element);
                    nextEltState.candidateList.add(new PathNode(nextPathNode));

                }

            } else {
                remainingNodes.add(pathNode);
            }
        }
        context.filterCheck = remainingNodes;
    }
}