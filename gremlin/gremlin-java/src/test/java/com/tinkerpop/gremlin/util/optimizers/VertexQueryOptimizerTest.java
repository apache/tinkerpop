package com.tinkerpop.gremlin.util.optimizers;

import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizerTest {

    @Test
    public void shouldPutHasParametersIntoVertexEdgeQueryBuilder() {
       /* GremlinJ<Vertex, Edge> gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.V().outE("knows").has("weight", 1.0f);
        assertEquals(3, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep);
        assertTrue(gremlin.getSteps().get(2) instanceof HasStep);
        assertEquals(Direction.OUT, ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.direction);
        assertEquals(1, ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.labels.length);
        assertEquals("knows", ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.labels[0]);
        assertEquals("weight", ((HasStep) gremlin.getSteps().get(2)).hasContainer.key);
        assertEquals(Compare.EQUAL, ((HasStep) gremlin.getSteps().get(2)).hasContainer.predicate);
        assertEquals(1.0f, ((HasStep) gremlin.getSteps().get(2)).hasContainer.value);
        assertTrue(gremlin.hasNext());
        assertEquals("8", gremlin.next().getId());
        assertFalse(gremlin.hasNext());

        gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.optimizers().register(new VertexQueryOptimizer());
        gremlin.V().outE("knows").has("weight", 1.0f);
        assertEquals(2, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep);
        assertEquals(Direction.OUT, ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.direction);
        assertEquals(1, ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.labels.length);
        assertEquals("knows", ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.labels[0]);
        assertEquals("weight", ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.hasContainers.get(0).key);
        assertEquals(Compare.EQUAL, ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.hasContainers.get(0).predicate);
        assertEquals(1.0f, ((VertexQueryStep) gremlin.getSteps().get(1)).queryBuilder.hasContainers.get(0).value);
        assertTrue(gremlin.hasNext());
        assertEquals("8", gremlin.next().getId());
        assertFalse(gremlin.hasNext());*/

    }

    @Test
    public void shouldReturnTheSameResultsAfterOptimization() {
     /*   GremlinJ a = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        a.optimizers().get().clear();
        a.V().outE("knows").has("weight", 1.0f);
        assertTrue(a.hasNext());

        GremlinJ b = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        b.optimizers().get().clear();
        b.optimizers().register(new VertexQueryOptimizer());
        b.V().outE("knows").has("weight", 1.0f);
        assertTrue(b.hasNext());

        assertEquals(a, b);
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());*/
    }

    @Test
    public void shouldNotRemoveEdgeVertexPipeIfTraversalIsGoingBackwards() {
     /*   GremlinJ gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.V().outE().outV();
        assertEquals(3, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep);
        assertTrue(gremlin.getSteps().get(2) instanceof EdgeVertexStep);

        gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.V().outE().outV();
        assertEquals(3, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep);
        assertTrue(gremlin.getSteps().get(2) instanceof EdgeVertexStep);

        gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.V().outE().bothV();
        assertEquals(3, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep);
        assertTrue(gremlin.getSteps().get(2) instanceof FlatMapStep); */
    }

    @Test
    public void shouldRemoveEdgeVertexPipeIfTraversalIsGoingForward() {
       /* GremlinJ gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.optimizers().get().clear();
        gremlin.V().outE().inV();
        assertEquals(3, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep);
        assertTrue(gremlin.getSteps().get(2) instanceof EdgeVertexStep);

        gremlin = (GremlinJ) GremlinJ.of(TinkerFactory.createClassic());
        gremlin.V().outE().inV();
        assertEquals(2, gremlin.getSteps().size());
        assertTrue(gremlin.getSteps().get(0) instanceof GraphQueryStep);
        assertTrue(gremlin.getSteps().get(1) instanceof VertexQueryStep); */
    }
}
