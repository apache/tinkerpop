package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTraversalTest {

    @Test
    public void shouldSubmitTraversalCorrectly() throws Exception {
        final Graph g = TinkerFactory.createClassic();
        final List<String> names = GroovyTraversal.<Vertex, String>of("g.V().out().out().value('name')").over(g).using(g.compute()).traversal().get().toList();
        assertEquals(2, names.size());
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
    }

    @Test
    public void shouldSubmitTraversalCorrectly2() throws Exception {
        final Graph g = TinkerFactory.createClassic();
        final List<String> names = GroovyTraversal.<Vertex, String>of("g.v(1).out().out().value('name')").over(g).using(g.compute()).traversal().get().toList();
        assertEquals(2, names.size());
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
    }
}
