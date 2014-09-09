package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HiddenValueMapTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMap();

    public abstract Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMapXageX();

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void g_V_hiddenValueMap() {
        final Traversal<Vertex, Map<String, List>> traversal = get_g_V_hiddenValueMap();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> hiddenValues = traversal.next();
            assertEquals(2, hiddenValues.size());
            assertEquals("hiddenName", hiddenValues.get("name").get(0));
            assertEquals(1, hiddenValues.get("name").size());
            assertEquals(-1, hiddenValues.get("age").get(0));
            assertEquals(1, hiddenValues.get("age").size());
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void g_V_hiddenValuesXageX() {
        final Traversal<Vertex, Map<String, List>> traversal = get_g_V_hiddenValueMapXageX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> hiddenValues = traversal.next();
            assertEquals(1, hiddenValues.size());
            assertEquals(-1, hiddenValues.get("age").get(0));
            assertEquals(1, hiddenValues.get("age").size());
        }
        assertEquals(6, counter);
    }

    public static class JavaHiddenValueMapTest extends HiddenValueMapTest {

        public JavaHiddenValueMapTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMap() {
            g.V().forEach(v -> {
                v.property(Graph.Key.hide("name"), "hiddenName");
                v.property(Graph.Key.hide("age"), -1);
            });
            return g.V().hiddenValueMap();
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMapXageX() {
            g.V().forEach(v -> {
                v.property(Graph.Key.hide("name"), "hiddenName");
                v.property(Graph.Key.hide("age"), -1);
            });
            return g.V().hiddenValueMap("age");
        }
    }
}