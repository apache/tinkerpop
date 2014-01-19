package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.pipes.filter.HasPipe;
import com.tinkerpop.gremlin.pipes.map.GraphQueryPipe;
import com.tinkerpop.gremlin.pipes.map.VertexEdgePipe;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizerTest {

    @Test
    public void shouldPutHasParametersIntoVertexEdgeQueryBuilder() {
        Gremlin<Vertex, Edge> gremlin = (Gremlin) Gremlin.of(TinkerFactory.createClassic());
        gremlin.getOptimizers().clear();
        gremlin.V().outE("knows").has("weight", 1.0f);

        assertEquals(3, gremlin.getPipes().size());
        assertTrue(gremlin.getPipes().get(0) instanceof GraphQueryPipe);
        assertTrue(gremlin.getPipes().get(1) instanceof VertexEdgePipe);
        assertTrue(gremlin.getPipes().get(2) instanceof HasPipe);
        assertEquals(Direction.OUT, ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.direction);
        assertEquals(1, ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.labels.length);
        assertEquals("knows", ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.labels[0]);
        assertEquals("weight", ((HasPipe) gremlin.getPipes().get(2)).hasContainer.key);
        assertEquals(Compare.EQUAL, ((HasPipe) gremlin.getPipes().get(2)).hasContainer.predicate);
        assertEquals(1.0f, ((HasPipe) gremlin.getPipes().get(2)).hasContainer.value);

        new VertexQueryOptimizer().optimize(gremlin);
        assertEquals(2, gremlin.getPipes().size());
        assertTrue(gremlin.getPipes().get(0) instanceof GraphQueryPipe);
        assertTrue(gremlin.getPipes().get(1) instanceof VertexEdgePipe);
        assertEquals(Direction.OUT, ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.direction);
        assertEquals(1, ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.labels.length);
        assertEquals("knows", ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.labels[0]);
        assertEquals("weight", ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.hasContainers.get(0).key);
        assertEquals(Compare.EQUAL, ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.hasContainers.get(0).predicate);
        assertEquals(1.0f, ((VertexEdgePipe) gremlin.getPipes().get(1)).queryBuilder.hasContainers.get(0).value);

        assertTrue(gremlin.hasNext());
        assertEquals("8", gremlin.next().getId());
        assertFalse(gremlin.hasNext());

    }

    @Test
    public void shouldReturnTheSameResultsAfterOptimization() {
        Gremlin a = (Gremlin) Gremlin.of(TinkerFactory.createClassic());
        a.getOptimizers().clear();
        a.V().outE("knows").has("weight", 1.0f);
        assertTrue(a.hasNext());

        Gremlin b = (Gremlin) Gremlin.of(TinkerFactory.createClassic());
        b.getOptimizers().clear();
        b.V().outE("knows").has("weight", 1.0f);
        new VertexQueryOptimizer().optimize(b);
        assertTrue(b.hasNext());

        assertEquals(a, b);
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
    }
}
