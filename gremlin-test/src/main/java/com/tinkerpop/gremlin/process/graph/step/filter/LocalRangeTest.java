package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class LocalRangeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Edge> get_g_V_outE_localRangeX0_2X();

    public abstract Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_localRangeX0_2X() {
        final Traversal<Vertex, Edge> traversal = get_g_V_outE_localRangeX0_2X();
        printTraversalForm(traversal);
        int counter = 0;
        final Map<Vertex, Long> map = new HashMap<>();
        while (traversal.hasNext()) {
            counter++;
            final Edge edge = traversal.next();
            MapHelper.incr(map, edge.iterators().vertexIterator(Direction.OUT).next(), 1l);
        }
        assertEquals(3, map.size());
        assertEquals(5, counter);
        map.forEach((k, v) -> {
            if (k.id().equals(convertToVertexId("marko"))) {
                assertEquals(2l, v.longValue());
            } else if (k.id().equals(convertToVertexId("josh"))) {
                assertEquals(2l, v.longValue());
            } else if (k.id().equals(convertToVertexId("peter"))) {
                assertEquals(1l, v.longValue());
            } else {
                fail("The following vertex should not have yielded edges: " + k);
            }
        });
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value() {
        final Traversal<Vertex, String> traversal = get_g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("brussels","san diego","centreville","dulles","baltimore","bremen","aachen","kaiserslautern"), traversal);

    }

    public static class StandardTest extends LocalRangeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_outE_localRangeX0_2X() {
            return g.V().outE().localRange(0, 2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value() {
            return g.V().properties("location").orderBy(T.value).localRange(0, 2).value();
        }
    }

    public static class ComputerTest extends LocalRangeTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_outE_localRangeX0_2X() {
            return g.V().outE().<Edge>localRange(0, 2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value() {
            return g.V().properties("location").orderBy(T.value).localRange(0, 2).<String>value(); // TODO:
        }
    }
}