package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexPropertyTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        final DetachedVertexProperty dvp = DetachedFactory.detach(vp, true);
        assertSame(dvp, DetachedFactory.detach(dvp, true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructDetachedPropertyWithPropertyFromVertex() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        final DetachedVertexProperty mp = DetachedFactory.detach(vp, true);
        assertEquals("test", mp.key());
        assertEquals("this", mp.value());
        assertEquals(DetachedVertex.class, mp.element().getClass());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructDetachedPropertyWithHiddenFromVertex() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        final DetachedVertexProperty mp = DetachedFactory.detach(vp, true);
        assertEquals("test", mp.key());
        assertEquals("this", mp.value());
        assertEquals(DetachedVertex.class, mp.element().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotSupportRemove() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        DetachedFactory.detach(vp, true).remove();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldBeEqualsPropertiesAsIdIsTheSame() {
        final Vertex v = g.addVertex();
        final VertexProperty vp = v.property("test", "this");
        final DetachedVertexProperty mp1 = DetachedFactory.detach(vp, true);
        final DetachedVertexProperty mp2 = DetachedFactory.detach(vp, true);
        assertTrue(mp1.equals(mp2));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotBeEqualsPropertiesAsIdIsDifferent() {
        final Vertex v = g.addVertex();
        final VertexProperty vp1 = v.property("test", "this");
        final DetachedVertexProperty mp1 = DetachedFactory.detach(vp1, true);
        final VertexProperty vp2 = v.property("testing", "this");
        final DetachedVertexProperty mp2 = DetachedFactory.detach(vp2, true);
        assertFalse(mp1.equals(mp2));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldDetachMultiPropertiesAndMetaProperties() {
        final Vertex v1 = convertToVertex(g, "marko");
        v1.iterators().propertyIterator("location").forEachRemaining(vp -> {
            final DetachedVertexProperty detached = DetachedFactory.detach(vp, true);
            if (detached.value().equals("san diego")) {
                assertEquals(1997, (int) detached.value("startTime"));
                assertEquals(2001, (int) detached.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(detached.iterators().propertyIterator()).count());
            } else if (vp.value().equals("santa cruz")) {
                assertEquals(2001, (int) detached.value("startTime"));
                assertEquals(2004, (int) detached.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(detached.iterators().propertyIterator()).count());
            } else if (detached.value().equals("brussels")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(detached.iterators().propertyIterator()).count());
            } else if (detached.value().equals("santa fe")) {
                assertEquals(2005, (int) detached.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(detached.iterators().propertyIterator()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAttachToGraph() {
        final Vertex v = g.addVertex();
        final VertexProperty toDetach = v.property("test", "this");
        final DetachedVertexProperty detached = DetachedFactory.detach(toDetach, true);
        final VertexProperty attached = detached.attach(g);

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedVertexProperty);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAttachToVertex() {
        final Vertex v = g.addVertex();
        final VertexProperty toDetach = v.property("test", "this");
        final DetachedVertexProperty detached = DetachedFactory.detach(toDetach, true);
        final VertexProperty attached = detached.attach(v);

        assertEquals(toDetach, attached);
        assertEquals(toDetach.getClass(), attached.getClass());
        assertFalse(attached instanceof DetachedVertexProperty);
    }
}
