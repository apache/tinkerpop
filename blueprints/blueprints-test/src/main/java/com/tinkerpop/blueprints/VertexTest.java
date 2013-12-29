package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.VertexFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexPropertyFeatures;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.blueprints.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_INTEGER_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexTest extends AbstractBlueprintsTest {

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    public void shouldSupportBasicVertexManipulation() {
        // test property mutation behaviors
        final Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals(34, (int) v.getValue("age"));
        assertEquals("marko", v.<String>getValue("name"));
        assertEquals(34, (int) v.getProperty("age").getValue());
        assertEquals("marko", v.<String>getProperty("name").getValue());
        assertEquals(2, v.getProperties().size());
        assertEquals(2, v.getPropertyKeys().size());
        assertTrue(v.getPropertyKeys().contains("name"));
        assertTrue(v.getPropertyKeys().contains("age"));
        assertFalse(v.getPropertyKeys().contains("location"));
        BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0);

        v.setProperty("name", "marko rodriguez");
        assertEquals(34, (int) v.getValue("age"));
        assertEquals("marko rodriguez", v.<String>getValue("name"));
        assertEquals(34, (int) v.getProperty("age").getValue());
        assertEquals("marko rodriguez", v.<String>getProperty("name").getValue());
        assertEquals(2, v.getProperties().size());
        assertEquals(2, v.getPropertyKeys().size());
        assertTrue(v.getPropertyKeys().contains("name"));
        assertTrue(v.getPropertyKeys().contains("age"));
        assertFalse(v.getPropertyKeys().contains("location"));
        BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0);

        v.setProperty("location", "santa fe");
        assertEquals(3, v.getProperties().size());
        assertEquals(3, v.getPropertyKeys().size());
        assertEquals("santa fe", v.getProperty("location").getValue());
        assertEquals(v.getProperty("location"), v.getProperty("location"));
        assertNotEquals(v.getProperty("location"), v.getProperty("name"));
        assertTrue(v.getPropertyKeys().contains("name"));
        assertTrue(v.getPropertyKeys().contains("age"));
        assertTrue(v.getPropertyKeys().contains("location"));
        v.getProperty("location").remove();
        BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0);
        assertEquals(2, v.getProperties().size());
        v.getProperties().values().stream().forEach(Property::remove);
        assertEquals(0, v.getProperties().size());
        BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0);
    }

    @Test
    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldEvaluateVerticesEquivalentWithSuppliedIds() {
        final Vertex v = g.addVertex(Property.Key.ID, BlueprintsStandardSuite.GraphManager.get().convertId("1"));
        final Vertex u = g.query().ids(BlueprintsStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next();
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
        final Vertex v = g.addVertex(Property.Key.ID, BlueprintsStandardSuite.GraphManager.get().convertId("1"));
        final Vertex u = g.query().ids(BlueprintsStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next();
        assertEquals(v, u);

        final Set<Vertex> set = new HashSet<>();
        set.add(v);
        set.add(v);
        set.add(u);
        set.add(u);
        set.add(g.query().ids(BlueprintsStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next());
        set.add(g.query().ids(BlueprintsStandardSuite.GraphManager.get().convertId("1")).vertices().iterator().next());

        assertEquals(1, set.size());
        assertEquals(v.hashCode(), u.hashCode());
    }

    @Test
    public void shouldCauseExceptionIfVertexRemovedMoreThanOnce() {
        Vertex v = g.addVertex();
        assertNotNull(v);

        Object id = v.getId();
        v.remove();
        assertFalse(g.query().ids(id).vertices().iterator().hasNext());

        // try second remove with no commit
        try {
            v.remove();
            fail("Vertex cannot be removed twice.");
        } catch (IllegalStateException ise) {
            assertEquals(Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, id).getMessage(), ise.getMessage());
        }

        v = g.addVertex();
        assertNotNull(v);

        id = v.getId();
        v.remove();

        // try second remove with a commit and then a second remove.  both should return the same exception
        tryCommit(g);

        try {
            v.remove();
            fail("Vertex cannot be removed twice.");
        } catch (IllegalStateException ise) {
            assertEquals(Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, id).getMessage(), ise.getMessage());
        }
    }
}
