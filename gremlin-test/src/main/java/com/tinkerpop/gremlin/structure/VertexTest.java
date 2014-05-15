package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.*;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexTest extends AbstractGremlinTest {

    @Test
    public void shouldHaveStandardStringRepresentation() {
        final Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals(StringFactory.vertexString(v), v.toString());
    }

    @Test
    public void shouldUseDefaultLabelIfNotSpecified() {
        final Vertex v = g.addVertex("name", "marko");
        assertEquals(Element.DEFAULT_LABEL, v.label());
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    public void shouldSupportBasicVertexManipulation() {
        // test property mutation behaviors
        final Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals(34, (int) v.value("age"));
        assertEquals("marko", v.<String>value("name"));
        assertEquals(34, (int) v.property("age").get());
        assertEquals("marko", v.<String>property("name").get());
        assertEquals(2, v.properties().size());
        assertEquals(2, v.keys().size());
        assertTrue(v.keys().contains("name"));
        assertTrue(v.keys().contains("age"));
        assertFalse(v.keys().contains("location"));
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);

        v.property("name", "marko rodriguez");
        assertEquals(34, (int) v.value("age"));
        assertEquals("marko rodriguez", v.<String>value("name"));
        assertEquals(34, (int) v.property("age").get());
        assertEquals("marko rodriguez", v.<String>property("name").get());
        assertEquals(2, v.properties().size());
        assertEquals(2, v.keys().size());
        assertTrue(v.keys().contains("name"));
        assertTrue(v.keys().contains("age"));
        assertFalse(v.keys().contains("location"));
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);

        v.property("location", "santa fe");
        assertEquals(3, v.properties().size());
        assertEquals(3, v.keys().size());
        assertEquals("santa fe", v.property("location").get());
        assertEquals(v.property("location"), v.property("location"));
        assertNotEquals(v.property("location"), v.property("name"));
        assertTrue(v.keys().contains("name"));
        assertTrue(v.keys().contains("age"));
        assertTrue(v.keys().contains("location"));
        v.property("location").remove();
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);
        assertEquals(2, v.properties().size());
        v.properties().values().stream().forEach(Property::remove);
        assertEquals(0, v.properties().size());
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);
    }

    @Test
    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldEvaluateVerticesEquivalentWithSuppliedIds() {
        final Vertex v = g.addVertex(Element.ID, GraphManager.get().convertId("1"));
        final Vertex u = g.v(GraphManager.get().convertId("1"));
        assertEquals(v, u);
    }

    @Test
    public void shouldEvaluateEquivalentVerticesWithNoSuppliedIds() {
        final Vertex v = g.addVertex();
        assertNotNull(v);

        final Vertex u = g.v(v.id());
        assertNotNull(u);
        assertEquals(v, u);

        assertEquals(g.v(u.id()), g.v(u.id()));
        assertEquals(g.v(v.id()), g.v(u.id()));
        assertEquals(g.v(v.id()), g.v(v.id()));
    }

    @Test
    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldEvaluateEquivalentVertexHashCodeWithSuppliedIds() {
        final Vertex v = g.addVertex(Element.ID, GraphManager.get().convertId("1"));
        final Vertex u = g.v(GraphManager.get().convertId("1"));
        assertEquals(v, u);

        final Set<Vertex> set = new HashSet<>();
        set.add(v);
        set.add(v);
        set.add(u);
        set.add(u);
        set.add(g.v(GraphManager.get().convertId("1")));
        set.add(g.v(GraphManager.get().convertId("1")));

        assertEquals(1, set.size());
        assertEquals(v.hashCode(), u.hashCode());
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldAutotypeStringProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.property("string", "marko");
        final String name = v.value("string");
        assertEquals(name, "marko");

    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    public void shouldAutotypIntegerProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.property("integer", 33);
        final Integer age = v.value("integer");
        assertEquals(Integer.valueOf(33), age);
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
    public void shouldAutotypeBooleanProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.property("boolean", true);
        final Boolean best = v.value("boolean");
        assertEquals(best, Boolean.valueOf(true));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
    public void shouldAutotypeDoubleProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.property("double", 0.1d);
        final Double best = v.value("double");
        assertEquals(best, Double.valueOf(0.1d));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
    public void shouldAutotypeLongProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.property("long", 1l);
        final Long best = v.value("long");
        assertEquals(best, Long.valueOf(1l));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    public void shouldAutotypeFloatProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.property("float", 0.1f);
        final Float best = v.value("float");
        assertEquals(best, Float.valueOf(0.1f));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldGetPropertyKeysOnVertex() {
        final Vertex v = g.addVertex("name", "marko", "location", "desert", "status", "dope");
        Set<String> keys = v.keys();
        assertEquals(3, keys.size());

        assertTrue(keys.contains("name"));
        assertTrue(keys.contains("location"));
        assertTrue(keys.contains("status"));

        final Map<String, Property> m = v.properties();
        assertEquals(3, m.size());
        assertEquals("name", m.get("name").getKey());
        assertEquals("location", m.get("location").getKey());
        assertEquals("status", m.get("status").getKey());
        assertEquals("marko", m.get("name").orElse(""));
        assertEquals("desert", m.get("location").orElse(""));
        assertEquals("dope", m.get("status").orElse(""));

        v.property("status").remove();

        keys = v.keys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("name"));
        assertTrue(keys.contains("location"));

        v.properties().values().stream().forEach(p -> p.remove());

        keys = v.keys();
        assertEquals(0, keys.size());
    }

    @Test
    public void shouldNotGetConcurrentModificationException() {
        for (int i = 0; i < 25; i++) {
            g.addVertex();
        }

        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(25, 0));

        for (Vertex v : g.V().toList()) {
            v.remove();
            tryCommit(g);
        }

        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(0, 0));
    }

    @Test
    public void shouldReturnEmptyMapIfNoProperties() {
        final Vertex v = g.addVertex();
        final Map<String,Property> m = v.properties();
        assertNotNull(m);
        assertEquals(0, m.size());
    }
}
