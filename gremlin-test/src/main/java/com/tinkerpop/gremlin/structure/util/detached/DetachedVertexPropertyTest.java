package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexPropertyTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        DetachedVertexProperty.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        DetachedVertexProperty.detach(DetachedVertexProperty.detach(vp));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructDetachedPropertyWithPropertyFromVertex() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        final DetachedVertexProperty mp = DetachedVertexProperty.detach(vp);
        assertEquals("test", mp.key());
        assertEquals("this", mp.value());
        assertFalse(mp.isHidden());
        assertEquals(DetachedVertex.class, mp.getElement().getClass());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructDetachedPropertyWithHiddenFromVertex() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property(Graph.Key.hide("test"), "this");
        final DetachedVertexProperty mp = DetachedVertexProperty.detach(vp);
        assertEquals("test", mp.key());
        assertEquals("this", mp.value());
        assertTrue(mp.isHidden());
        assertEquals(DetachedVertex.class, mp.getElement().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotSupportRemove() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        DetachedVertexProperty.detach(vp).remove();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldBeEqualsPropertiesAsIdIsTheSame() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        final DetachedVertexProperty mp1 = DetachedVertexProperty.detach(vp);
        final DetachedVertexProperty mp2 = DetachedVertexProperty.detach(vp);
        assertTrue(mp1.equals(mp2));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotBeEqualsPropertiesAsIdIsDifferent() {
        final Vertex v = g.addVertex();
        final VertexProperty vp1 = v.property("test", "this");
        final DetachedVertexProperty mp1 = DetachedVertexProperty.detach(vp1);
        final VertexProperty vp2 = v.property("testing", "this");
        final DetachedVertexProperty mp2 = DetachedVertexProperty.detach(vp2);
        assertFalse(mp1.equals(mp2));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldDetachMultiPropertiesAndMetaProperties() {
        final Vertex v1 = convertToVertex(g, "marko");
        v1.iterators().properties("location").forEachRemaining(vp -> {
            final DetachedVertexProperty detached = DetachedVertexProperty.detach(vp);
            if (detached.value().equals("san diego")) {
                assertEquals(1997, (int) detached.value("startTime"));
                assertEquals(2001, (int) detached.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(detached.iterators().properties()).count());
            } else if (vp.value().equals("santa cruz")) {
                assertEquals(2001, (int) detached.value("startTime"));
                assertEquals(2004, (int) detached.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(detached.iterators().properties()).count());
            } else if (detached.value().equals("brussels")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(detached.iterators().properties()).count());
            } else if (detached.value().equals("santa fe")) {
                assertEquals(2005, (int) detached.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(detached.iterators().properties()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
    }

}
