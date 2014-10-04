package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTraversalScriptTest extends AbstractGremlinTest {

    // todo: review for other required features???

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldSubmitTraversalCorrectly() throws Exception {
        final List<String> names = GroovyTraversalScript.<Vertex, String>of("g.V().out().out().value('name')").over(g).using(g.compute()).traversal().get().toList();
        assertEquals(2, names.size());
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldSubmitTraversalCorrectly2() throws Exception {
        final List<String> names = GroovyTraversalScript.<Vertex, String>of("g.v(1).out().out().value('name')").over(g).using(g.compute()).traversal().get().toList();
        assertEquals(2, names.size());
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
    }
}
