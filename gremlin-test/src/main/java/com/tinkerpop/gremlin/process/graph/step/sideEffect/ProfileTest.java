package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.GlobalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProfileTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, GlobalMetrics> get_g_V_out_out_profile();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_profile() {
        final Traversal<Vertex, GlobalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);
        GlobalMetrics metrics = traversal.next();
        // TODO: validate the metrics data
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends ProfileTest {

        @Override
        public Traversal<Vertex, GlobalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }
    }

    public static class ComputerTest extends ProfileTest {

        @Override
        public Traversal<Vertex, GlobalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile().submit(g.compute());
        }
    }
}