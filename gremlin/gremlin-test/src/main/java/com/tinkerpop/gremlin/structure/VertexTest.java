package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_BOOLEAN_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_DOUBLE_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_FLOAT_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_LONG_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    public void shouldSupportBasicVertexManipulation() {
        // test property mutation behaviors
        final Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals(34, (int) v.getValue("age"));
        assertEquals("marko", v.<String>getValue("name"));
        assertEquals(34, (int) v.getProperty("age").get());
        assertEquals("marko", v.<String>getProperty("name").get());
        assertEquals(2, v.getProperties().size());
        assertEquals(2, v.getPropertyKeys().size());
        assertTrue(v.getPropertyKeys().contains("name"));
        assertTrue(v.getPropertyKeys().contains("age"));
        assertFalse(v.getPropertyKeys().contains("location"));
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);

        v.setProperty("name", "marko rodriguez");
        assertEquals(34, (int) v.getValue("age"));
        assertEquals("marko rodriguez", v.<String>getValue("name"));
        assertEquals(34, (int) v.getProperty("age").get());
        assertEquals("marko rodriguez", v.<String>getProperty("name").get());
        assertEquals(2, v.getProperties().size());
        assertEquals(2, v.getPropertyKeys().size());
        assertTrue(v.getPropertyKeys().contains("name"));
        assertTrue(v.getPropertyKeys().contains("age"));
        assertFalse(v.getPropertyKeys().contains("location"));
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);

        v.setProperty("location", "santa fe");
        assertEquals(3, v.getProperties().size());
        assertEquals(3, v.getPropertyKeys().size());
        assertEquals("santa fe", v.getProperty("location").get());
        assertEquals(v.getProperty("location"), v.getProperty("location"));
        assertNotEquals(v.getProperty("location"), v.getProperty("name"));
        assertTrue(v.getPropertyKeys().contains("name"));
        assertTrue(v.getPropertyKeys().contains("age"));
        assertTrue(v.getPropertyKeys().contains("location"));
        v.getProperty("location").remove();
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);
        assertEquals(2, v.getProperties().size());
        v.getProperties().values().stream().forEach(Property::remove);
        assertEquals(0, v.getProperties().size());
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);
    }

    @Test
    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldEvaluateVerticesEquivalentWithSuppliedIds() {
        final Vertex v = g.addVertex(Element.ID, StructureStandardSuite.GraphManager.get().convertId("1"));
        final Vertex u = g.query().ids(StructureStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next();
        assertEquals(v, u);
    }

    @Test
    public void shouldEvaluateEquivalentVerticesWithNoSuppliedIds() {
        final Vertex v = g.addVertex();
        assertNotNull(v);

        final Vertex u = g.query().ids(v.getId()).vertices().iterator().next();
        assertNotNull(u);
        assertEquals(v, u);

        assertEquals(g.query().ids(u.getId()).vertices().iterator().next(), g.query().ids(u.getId()).vertices().iterator().next());
        assertEquals(g.query().ids(v.getId()).vertices().iterator().next(), g.query().ids(u.getId()).vertices().iterator().next());
        assertEquals(g.query().ids(v.getId()).vertices().iterator().next(), g.query().ids(v.getId()).vertices().iterator().next());
    }

    @Test
    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldEvaluateEquivalentVertexHashCodeWithSuppliedIds() {
        final Vertex v = g.addVertex(Element.ID, StructureStandardSuite.GraphManager.get().convertId("1"));
        final Vertex u = g.query().ids(StructureStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next();
        assertEquals(v, u);

        final Set<Vertex> set = new HashSet<>();
        set.add(v);
        set.add(v);
        set.add(u);
        set.add(u);
        set.add(g.query().ids(StructureStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next());
        set.add(g.query().ids(StructureStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next());

        assertEquals(1, set.size());
        assertEquals(v.hashCode(), u.hashCode());
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldAutotypeStringProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.setProperty("string", "marko");
        final String name = v.getValue("string");
        assertEquals(name, "marko");

    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    public void shouldAutotypIntegerProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.setProperty("integer", 33);
        final Integer age = v.getValue("integer");
        assertEquals(Integer.valueOf(33), age);
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
    public void shouldAutotypeBooleanProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.setProperty("boolean", true);
        final Boolean best = v.getValue("boolean");
        assertEquals(best, Boolean.valueOf(true));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
    public void shouldAutotypeDoubleProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.setProperty("double", 0.1d);
        final Double best = v.getValue("double");
        assertEquals(best, Double.valueOf(0.1d));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
    public void shouldAutotypeLongProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.setProperty("long", 1l);
        final Long best = v.getValue("long");
        assertEquals(best, Long.valueOf(1l));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    public void shouldAutotypeFloatProperties() {
        final Graph graph = g;
        final Vertex v = graph.addVertex();
        v.setProperty("float", 0.1f);
        final Float best = v.getValue("float");
        assertEquals(best, Float.valueOf(0.1f));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldGetPropertyKeysOnVertex() {
        final Vertex v = g.addVertex("name", "marko", "location", "desert", "status", "dope");
        Set<String> keys = v.getPropertyKeys();
        assertEquals(3, keys.size());

        assertTrue(keys.contains("name"));
        assertTrue(keys.contains("location"));
        assertTrue(keys.contains("status"));

        final Map<String, Property> m = v.getProperties();
        assertEquals(3, m.size());
        assertEquals("name", m.get("name").getKey());
        assertEquals("location", m.get("location").getKey());
        assertEquals("status", m.get("status").getKey());
        assertEquals("marko", m.get("name").orElse(""));
        assertEquals("desert", m.get("location").orElse(""));
        assertEquals("dope", m.get("status").orElse(""));

        v.getProperty("status").remove();

        keys = v.getPropertyKeys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("name"));
        assertTrue(keys.contains("location"));

        v.getProperties().values().stream().forEach(p -> p.remove());

        keys = v.getPropertyKeys();
        assertEquals(0, keys.size());
    }

    @Test
    public void shouldNotGetConcurrentModificationException() {
        for (int i = 0; i < 25; i++) {
            g.addVertex();
        }

        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(25, 0));

        for (Vertex v : g.query().vertices()) {
            v.remove();
            tryCommit(g);
        }

        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(0, 0));
    }
}
