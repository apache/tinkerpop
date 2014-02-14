package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * An extension of the {@link AbstractGremlinTest} that loads the test graph instance with a particular graph dataset.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AbstractToyGraphGremlinTest extends AbstractGremlinTest {

    @Override
    protected void prepareGraph(final Graph g) throws Exception {
        final String testName = name.getMethodName();
        final Method method = this.getClass().getMethod(testName);
        final Optional<LoadGraphWith> loadGraphWith = Optional.ofNullable(method.getAnnotation(LoadGraphWith.class));
        final String path;
        if (loadGraphWith.isPresent())
            path = loadGraphWith.get().value().location();
        else
            path = LoadGraphWith.GraphData.CLASSIC.location();

        readGraphMLIntoGraph(g, path);
    }

    private static void readGraphMLIntoGraph(final Graph g, final String path) throws IOException {
        final GraphReader reader = new GraphMLReader.Builder(g).build();
        try (final InputStream stream = AbstractToyGraphGremlinTest.class.getResourceAsStream(path)) {
            reader.inputGraph(stream);
        }
    }
}
