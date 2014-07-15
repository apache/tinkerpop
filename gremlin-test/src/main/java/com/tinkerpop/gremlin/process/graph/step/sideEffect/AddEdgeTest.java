package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AddEdgeTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, Vertex> get_g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX() {
        final Iterator<Vertex> step = get_g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        final List<Vertex> cocreators = new ArrayList<>();
        final List<Object> ids = new ArrayList<>();
        while (step.hasNext()) {
            final Vertex vertex = step.next();
            cocreators.add(vertex);
            ids.add(vertex.id());
        }
        assertEquals(cocreators.size(), 3);
        assertTrue(ids.contains(convertToVertexId("marko")));
        assertTrue(ids.contains(convertToVertexId("peter")));
        assertTrue(ids.contains(convertToVertexId("josh")));

        for (Vertex vertex : cocreators) {
            if (vertex.id().equals(convertToVertexId("marko"))) {
                assertEquals(vertex.outE("cocreator").count().next(), new Long(4));
                assertEquals(vertex.inE("cocreator").count().next(), new Long(4));
            } else {
                assertEquals(vertex.outE("cocreator").count().next(), new Long(1));
                assertEquals(vertex.inE("cocreator").count().next(), new Long(1));
            }
        }
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX() {
        final Iterator<Vertex> step = get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        int count = 0;
        while (step.hasNext()) {
            final Vertex vertex = step.next();
            assertEquals(convertToVertexId("lop"), vertex.id());
            assertEquals(Long.valueOf(1l), vertex.out("createdBy").count().next());
            assertEquals(convertToVertexId("marko"), vertex.out("createdBy").id().next());
            count++;

        }
        assertEquals(1, count);
    }

    public static class JavaAddEdgeTest extends AddEdgeTest {

        public Traversal<Vertex, Vertex> get_g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(final Object v1Id) {
            return g.v(v1Id).as("a").out("created").in("created").addBothE("cocreator", "a");
        }

        public Traversal<Vertex, Vertex> get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            return g.v(v1Id).as("a").out("created").addOutE("createdBy", "a");
        }
    }
}
