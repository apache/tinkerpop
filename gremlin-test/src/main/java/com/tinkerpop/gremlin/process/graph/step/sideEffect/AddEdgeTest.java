package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC_DOUBLE;
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

    public abstract Traversal<Vertex, Vertex> get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC_DOUBLE)
    @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Vertex> cocreators = new ArrayList<>();
        final List<Object> ids = new ArrayList<>();
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
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
    @LoadGraphWith(CLASSIC_DOUBLE)
    @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals(convertToVertexId("lop"), vertex.id());
            assertEquals(1, vertex.out("createdBy").count().next().longValue());
            assertEquals(convertToVertexId("marko"), vertex.out("createdBy").id().next());
            assertEquals(0, vertex.outE("createdBy").values().next().size());
            count++;

        }
        assertEquals(1, count);
    }

    @Test
    @LoadGraphWith(CLASSIC_DOUBLE)
    @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals(convertToVertexId("lop"), vertex.id());
            assertEquals(Long.valueOf(1l), vertex.out("createdBy").count().next());
            assertEquals(convertToVertexId("marko"), vertex.out("createdBy").id().next());
            assertEquals(2, vertex.outE("createdBy").value("weight").next());
            assertEquals(1, vertex.outE("createdBy").values().next().size());
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

        public Traversal<Vertex, Vertex> get_g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id) {
            return g.v(v1Id).as("a").out("created").addOutE("createdBy", "a", "weight", 2);
        }
    }
}
