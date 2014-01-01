package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Holder of {@link com.tinkerpop.blueprints.Graph} instances configured for the server to be passed to
 * sessionless bindings.
 */
public class Graphs {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);

    private final Map<String, Graph> graphs;

    public Graphs(final Settings settings) {
        final Map<String, Graph> m = new HashMap<>();
        settings.graphs.entrySet().forEach(e -> {
            try {
                final Graph newGraph = GraphFactory.open(e.getValue());
                m.put(e.getKey(), newGraph);
                logger.info("Graph [{}] was successfully configured via [{}].", e.getKey(), e.getValue());
            } catch (RuntimeException re) {
                logger.warn("Graph [{}] configured at [{}] could not be instantiated and will not be available in Gremlin Server.  GraphFactory message: {}",
                        e.getKey(), e.getValue(), re.getMessage());
                if (logger.isDebugEnabled() && re.getCause() != null) logger.debug("GraphFactory exception", re.getCause());
            }
        });
        graphs = Collections.unmodifiableMap(m);
    }

    public Map<String, Graph> getGraphs() {
        return graphs;
    }

    /**
     * Rollback transactions across all {@link com.tinkerpop.blueprints.Graph} objects.
     */
    public void rollbackAll() {
        graphs.entrySet().forEach(e-> {
            final Graph g = e.getValue();
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().rollback();
        });
    }

    /**
     * Commit transactions across all {@link com.tinkerpop.blueprints.Graph} objects.
     */
    public void commitAll() {
        graphs.entrySet().forEach(e->{
            final Graph g = e.getValue();
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().commit();
        });
    }
}
