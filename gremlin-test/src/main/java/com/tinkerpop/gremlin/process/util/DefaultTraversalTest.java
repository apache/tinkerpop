package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalTest {

    @Test
    public void shouldCloneTraversalCorrectly() throws CloneNotSupportedException {
        final Graph g = EmptyGraph.instance();
        final DefaultGraphTraversal<?, ?> original = new DefaultGraphTraversal<>(g.getClass());
        original.out().groupCount("m").values("name").count();
        final DefaultTraversal<?, ?> clone = (DefaultTraversal) original.clone();
        assertNotEquals(original.hashCode(), clone.hashCode());
        assertEquals(original.getSteps().size(), clone.getSteps().size());

        for (int i = 0; i < original.steps.size(); i++) {
            assertNotEquals(original.getSteps().get(i), clone.getSteps().get(i));
        }
        assertNotEquals(original.sideEffects, clone.sideEffects);

    }
}
