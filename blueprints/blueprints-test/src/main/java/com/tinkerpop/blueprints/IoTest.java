package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.io.GraphReader;
import com.tinkerpop.blueprints.io.graphml.GraphMLReader;
import com.tinkerpop.blueprints.util.StreamFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoTest {
    @Test
    public void shouldReadGraphML() throws IOException {
        final Graph g = BlueprintsSuite.GraphManager.get().newTestGraph();
        final GraphReader reader = new GraphMLReader.Builder(g).build();

        try (final InputStream stream = IoTest.class.getResourceAsStream("/com/tinkerpop/blueprints/util/io/graphml/graph-example-1.xml")) {
            reader.inputGraph(stream);
        }

        assertEquals(6, StreamFactory.stream(g.query().vertices()).count());
        assertEquals(6, StreamFactory.stream(g.query().edges()).count());
    }
}
