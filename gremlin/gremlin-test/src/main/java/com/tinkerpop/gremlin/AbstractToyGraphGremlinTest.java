package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * An extension of the {@link AbstractGremlinTest} that loads the test graph instance with a particular graph dataset.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AbstractToyGraphGremlinTest extends AbstractGremlinTest {

    private static final String RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/graphml/";

    @Override
    protected void prepareGraph(final Graph g) throws Exception {
        readGraphMLIntoGraph(g);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = new GraphMLReader.Builder(g).build();
        try (final InputStream stream = AbstractToyGraphGremlinTest.class.getResourceAsStream(RESOURCE_PATH_PREFIX + "graph-example-1.xml")) {
            reader.inputGraph(stream);
        }
    }
}
