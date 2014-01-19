package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.pipes.filter.HasPipe;
import com.tinkerpop.gremlin.pipes.map.GraphQueryPipe;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizerTest {

    @Test
    public void shouldPutHasParametersIntoGraphQueryBuilder() {
        Gremlin<Vertex, Vertex> gremlin = (Gremlin) Gremlin.of(TinkerFactory.createClassic());
        gremlin.getOptimizers().clear();
        gremlin.V().has("age", 29);

        assertEquals(2, gremlin.getPipes().size());
        assertTrue(gremlin.getPipes().get(0) instanceof GraphQueryPipe);
        assertTrue(gremlin.getPipes().get(1) instanceof HasPipe);
        assertEquals("age", ((HasPipe) gremlin.getPipes().get(1)).hasContainer.key);
        assertEquals(Compare.EQUAL, ((HasPipe) gremlin.getPipes().get(1)).hasContainer.predicate);
        assertEquals(29, ((HasPipe) gremlin.getPipes().get(1)).hasContainer.value);

        new GraphQueryOptimizer().optimize(gremlin);
        assertEquals(1, gremlin.getPipes().size());
        assertTrue(gremlin.getPipes().get(0) instanceof GraphQueryPipe);
        assertEquals("age", ((GraphQueryPipe) gremlin.getPipes().get(0)).queryBuilder.hasContainers.get(0).key);
        assertEquals(Compare.EQUAL, ((GraphQueryPipe) gremlin.getPipes().get(0)).queryBuilder.hasContainers.get(0).predicate);
        assertEquals(29, ((GraphQueryPipe) gremlin.getPipes().get(0)).queryBuilder.hasContainers.get(0).value);

        assertEquals("marko", gremlin.next().<String>getValue("name"));
        assertFalse(gremlin.hasNext());
    }

    @Test
    public void shouldReturnTheSameResultsAfterOptimization() {
        Gremlin a = (Gremlin) Gremlin.of(TinkerFactory.createClassic());
        a.getOptimizers().clear();
        a.V().has("age", 29);
        assertTrue(a.hasNext());

        Gremlin b = (Gremlin) Gremlin.of(TinkerFactory.createClassic());
        b.getOptimizers().clear();
        b.V().has("age", 29);
        new GraphQueryOptimizer().optimize(b);
        assertTrue(b.hasNext());

        assertEquals(a, b);
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());

    }
}