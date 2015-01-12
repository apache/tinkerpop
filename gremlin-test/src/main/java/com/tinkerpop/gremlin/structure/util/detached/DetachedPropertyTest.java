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
import org.junit.Test;

import static org.junit.Assert.*;

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
        final DetachedProperty p = DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight"));
        assertEquals("weight", p.key());
        assertEquals(0.4d, (double) p.value(), 0.000001d);
        assertEquals(DetachedEdge.class, p.element().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotSupportRemove() {
        DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight")).remove();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeEqualProperties() {
        assertTrue(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight")).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Edge e = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Property toDetach = e.iterators().propertyIterator("weight").next();
        final DetachedProperty detachedProperty = DetachedFactory.detach(toDetach);
        final Property attached = detachedProperty.attach(g);

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedProperty);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Edge e = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Property toDetach = e.iterators().propertyIterator("weight").next();
        final DetachedProperty detachedProperty = DetachedFactory.detach(toDetach);
        final Property attached = detachedProperty.attach(e.iterators().vertexIterator(Direction.OUT).next());

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedProperty);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotBeEqualPropertiesAsThereIsDifferentId() {
        assertFalse(DetachedFactory.detach(g.E(convertToEdgeId("marko", "created", "lop")).next().property("weight")).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotBeEqualPropertiesAsThereIsDifferentKey() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Edge e = g.V(convertToVertexId("josh")).next().addEdge("created", g.V(convertToVertexId("lop")).next(), "weight", 0.4d);
        assertFalse(DetachedFactory.detach(e.property("weight")).equals(DetachedFactory.detach(g.E(joshCreatedLopEdgeId).next().property("weight"))));
    }
}
