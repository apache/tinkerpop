package com.tinkerpop.gremlin.util.optimizers;

import org.junit.Test;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizerTest {

    @Test
    public void shouldPutHasParametersIntoGraphQueryBuilder() {
     /*   GremlinJ<Vertex, Vertex> gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.V().has("age", 29);
        assertEquals(2, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof HasStep);
        assertEquals("age", ((HasStep) gremlin.getSteps().get(1)).hasContainer.key);
        assertEquals(Compare.EQUAL, ((HasStep) gremlin.getSteps().get(1)).hasContainer.predicate);
        assertEquals(29, ((HasStep) gremlin.getSteps().get(1)).hasContainer.value);
        assertEquals("marko", gremlin.next().<String>getValue("name"));
        assertFalse(gremlin.hasNext());

        gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.optimizers().register(new GraphQueryOptimizer());
        gremlin.V().has("age", 29);
        assertEquals(1, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertEquals("age", ((GraphQueryStep) gremlin.getSteps().get(0)).queryBuilder.hasContainers.get(0).key);
        assertEquals(Compare.EQUAL, ((GraphQueryStep) gremlin.getSteps().get(0)).queryBuilder.hasContainers.get(0).predicate);
        assertEquals(29, ((GraphQueryStep) gremlin.getSteps().get(0)).queryBuilder.hasContainers.get(0).value);

        assertEquals("marko", gremlin.next().<String>getValue("name"));
        assertFalse(gremlin.hasNext());   */
    }

    @Test
    public void shouldReturnTheSameResultsAfterOptimization() {
        /*GremlinJ a = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
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
        assertFalse(b.hasNext());*/

    }
}