package com.tinkerpop.gremlin;

/**
 * Holds the GraphProvider specified by the implementer using the StructureSuite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphManager {
    private static GraphProvider graphProvider;

    public static GraphProvider set(final GraphProvider graphProvider) {
        final GraphProvider old = GraphManager.graphProvider;
        GraphManager.graphProvider = graphProvider;
        return old;
    }

    public static GraphProvider get() {
        return graphProvider;
    }
}
