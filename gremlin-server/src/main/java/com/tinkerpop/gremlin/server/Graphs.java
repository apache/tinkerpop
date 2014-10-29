package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.Map;

/**
 * Traverser of {@link Graph} instances configured for the server to be passed to sessionless bindings. The
 * {@link Graph} instances are read from the {@link Settings} for Gremlin Server as defined in the configuration
 * file.
 */
public class Graphs {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);

    private final Map<String, Graph> graphs = new HashMap<>();

    /**
     * Create a new instance using the {@link Settings} from Gremlin Server.
     */
    public Graphs(final Settings settings) {
        settings.graphs.entrySet().forEach(e -> {
            try {
                final Graph newGraph = GraphFactory.open(e.getValue());
                graphs.put(e.getKey(), newGraph);
                logger.info("Graph [{}] was successfully configured via [{}].", e.getKey(), e.getValue());
            } catch (RuntimeException re) {
                logger.warn(String.format("Graph [%s] configured at [%s] could not be instantiated and will not be available in Gremlin Server.  GraphFactory message: %s",
                        e.getKey(), e.getValue(), re.getMessage()), re);
                if (re.getCause() != null) logger.debug("GraphFactory exception", re.getCause());
            }
        });
    }

    /**
     * Get a list of the {@link Graph} instances and their binding names as defined in the Gremlin Server
     * configuration file.  This {@link Map} is immutable.
     *
     * @return a {@link Map} where the key is the name of the {@link Graph} and the value is the {@link Graph} itself
     */
    public Map<String, Graph> getGraphs() {
        return graphs;
    }

    /**
     * Get the graphs list as a set of bindings.
     */
    public Bindings getGraphsAsBindings() {
        final Bindings bindings = new SimpleBindings();
        graphs.forEach(bindings::put);
        return bindings;
    }

    /**
     * Rollback transactions across all {@link com.tinkerpop.gremlin.structure.Graph} objects.
     */
    public void rollbackAll() {
        graphs.entrySet().forEach(e -> {
            final Graph g = e.getValue();
            if (g.features().graph().supportsTransactions())
                g.tx().rollback();
        });
    }

    /**
     * Commit transactions across all {@link com.tinkerpop.gremlin.structure.Graph} objects.
     */
    public void commitAll() {
        graphs.entrySet().forEach(e -> {
            final Graph g = e.getValue();
            if (g.features().graph().supportsTransactions())
                g.tx().commit();
        });
    }
}