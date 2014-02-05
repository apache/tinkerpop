package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizerTest {

    @Test
    public void shouldPutHasParametersIntoGraphQueryBuilder() {
        GremlinJ<Vertex, Vertex> gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.V().has("age", 29);
        assertEquals(2, gremlin.getPipes().size());
        assertTrue(gremlin.getPipes().get(0) instanceof GraphQueryPipe);
        assertTrue(gremlin.getPipes().get(1) instanceof HasPipe);
        assertEquals("age", ((HasPipe) gremlin.getPipes().get(1)).hasContainer.key);
        assertEquals(Compare.EQUAL, ((HasPipe) gremlin.getPipes().get(1)).hasContainer.predicate);
        assertEquals(29, ((HasPipe) gremlin.getPipes().get(1)).hasContainer.value);
        assertEquals("marko", gremlin.next().<String>getValue("name"));
        assertFalse(gremlin.hasNext());

        gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.optimizers().register(new GraphQueryOptimizer());
        gremlin.V().has("age", 29);
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
        GremlinJ a = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        a.optimizers().get().clear();
        a.V().has("age", 29);
        assertTrue(a.hasNext());

        GremlinJ b = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        b.optimizers().get().clear();
        b.optimizers().register(new GraphQueryOptimizer());
        b.V().has("age", 29);
        assertTrue(b.hasNext());

        assertEquals(a, b);
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());

    }
}