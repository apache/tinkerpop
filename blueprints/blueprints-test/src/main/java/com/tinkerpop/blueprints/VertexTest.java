package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.PropertyFeatures;
import org.junit.Test;

import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_INTEGER_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexTest extends AbstractBlueprintsTest {

    @Test
    @FeatureRequirement(featureClass = PropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = PropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
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
        BlueprintsSuite.assertVertexEdgeCounts(g, 1, 0);

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
        BlueprintsSuite.assertVertexEdgeCounts(g, 1, 0);

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
        BlueprintsSuite.assertVertexEdgeCounts(g, 1, 0);
        assertEquals(2, v.getProperties().size());
        v.getProperties().values().stream().forEach(i -> i.forEach(Property::remove));
        assertEquals(0, v.getProperties().size());
        BlueprintsSuite.assertVertexEdgeCounts(g, 1, 0);
    }
}
