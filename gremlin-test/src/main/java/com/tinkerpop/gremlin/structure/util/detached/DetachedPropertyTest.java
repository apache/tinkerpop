package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedPropertyTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        DetachedProperty.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v, "xxx", "yyy");
        DetachedProperty.detach(DetachedProperty.detach(e.property("xxx")));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldConstructDetachedPropertyWithPropertyFromEdge() {
        final DetachedProperty p = DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"));
        assertEquals("weight", p.key());
        assertEquals(0.4d, (double) p.value(), 0.000001d);
        assertEquals(DetachedEdge.class, p.getElement().getClass());
        assertFalse(p.isHidden());
    }

    // todo: test hidden with "the crew"

    @Test(expected = UnsupportedOperationException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotSupportRemove() {
        DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight")).remove();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeEqualsProperties() {
       assertTrue(DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight")).equals(DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentId() {
        assertFalse(DetachedProperty.detach(g.e(convertToEdgeId("marko", "created", "lop")).property("weight")).equals(DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentKey() {
        final Edge e = g.v(convertToVertexId("josh")).addEdge("created", g.v(convertToVertexId("lop")), Graph.Key.hide("weight"), 0.4d);
        assertFalse(DetachedProperty.detach(e.property(Graph.Key.hide("weight"))).equals(DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentValue() {
        final Edge e = g.v(4).addEdge("created", g.v(convertToVertexId("josh")), "weight", 123.0001d);
        assertFalse(DetachedProperty.detach(e.property("weight")).equals(DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }
}
