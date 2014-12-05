package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedPropertyTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v, "xxx", "yyy");
        final DetachedProperty dp = DetachedFactory.detach(e.property("xxx"));
        assertSame(dp, DetachedFactory.detach(dp));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldConstructDetachedPropertyWithPropertyFromEdge() {
        final DetachedProperty p = DetachedFactory.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"));
        assertEquals("weight", p.key());
        assertEquals(0.4d, (double) p.value(), 0.000001d);
        assertEquals(DetachedEdge.class, p.element().getClass());
        assertFalse(p.isHidden());
    }

    @Test(expected = UnsupportedOperationException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotSupportRemove() {
        DetachedFactory.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight")).remove();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeEqualProperties() {
       assertTrue(DetachedFactory.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight")).equals(DetachedFactory.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotBeEqualPropertiesAsThereIsDifferentId() {
        assertFalse(DetachedFactory.detach(g.e(convertToEdgeId("marko", "created", "lop")).property("weight")).equals(DetachedFactory.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotBeEqualPropertiesAsThereIsDifferentKey() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Edge e = g.v(convertToVertexId("josh")).addEdge("created", g.v(convertToVertexId("lop")), Graph.Key.hide("weight"), 0.4d);
        assertFalse(DetachedFactory.detach(e.property(Graph.Key.hide("weight"))).equals(DetachedFactory.detach(g.e(joshCreatedLopEdgeId).property("weight"))));
    }
}
