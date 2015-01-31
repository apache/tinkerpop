package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final DetachedVertex dv = DetachedFactory.detach(v, true);
        assertSame(dv, DetachedFactory.detach(dv, true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructDetachedVertex() {
        final Vertex v = g.addVertex("test", "123");
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);

        assertEquals(v.id(), detachedVertex.id());
        assertEquals(v.label(), detachedVertex.label());
        assertEquals("123", detachedVertex.value("test"));
        assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructDetachedVertexAsReference() {
        final Vertex v = g.addVertex("test", "123", "test", "321");
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, false);

        assertEquals(v.id(), detachedVertex.id());
        assertEquals(v.label(), detachedVertex.label());
        assertEquals(0, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldDetachVertexWithMultiPropertiesAndMetaProperties() {
        final DetachedVertex v1 = DetachedFactory.detach(convertToVertex(g, "marko"), true);

        assertEquals("person", v1.label());
        assertEquals(2, v1.keys().size());
        v1.iterators().propertyIterator("location").forEachRemaining(vp -> {
            assertTrue(vp instanceof DetachedVertexProperty);
            if (vp.value().equals("san diego")) {
                assertEquals(1997, (int) vp.value("startTime"));
                assertEquals(2001, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("santa cruz")) {
                assertEquals(2001, (int) vp.value("startTime"));
                assertEquals(2004, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("brussels")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("santa fe")) {
                assertEquals(2005, (int) vp.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).equals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true)));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).hashCode(), DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).hashCode());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Vertex toDetach = g.V(convertToVertexId("josh")).next();
        final DetachedVertex detachedVertex = DetachedFactory.detach(toDetach, true);
        final Vertex attached = detachedVertex.attach(g);

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Vertex toDetach = g.V(convertToVertexId("josh")).next();
        final DetachedVertex detachedVertex = DetachedFactory.detach(toDetach, true);
        final Vertex attached = detachedVertex.attach(toDetach);

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_INTEGER_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final DetachedVertex originalMarko = DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true);
        final Vertex secondMarko = g.addVertex("name", "marko", "age", 29);
        assertFalse(DetachedFactory.detach(secondMarko, true).equals(originalMarko));
    }

    @Test
    public void shouldConstructDetachedVertexFromParts() {
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", "x");
        final Map<String, Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", "x");
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String, Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", "y");
        final Map<String, Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", "y");
        properties.put("y", Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<VertexProperty> propertyX = StreamFactory.stream(dv.iterators().propertyIterator("x")).collect(Collectors.toList());
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
                p.label().equals(p.key())
                        && (p.id().equals(123) || p.id().equals(124))
                        && (p.value().equals("a") || p.value().equals("c"))
                        && !p.iterators().propertyIterator().hasNext()));

    }

    @Test
    public void shouldConstructDetachedVertexFromPartsWithPropertiesOnProperties() {
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", "x");
        propX1.put("properties", ElementHelper.asMap("propX1a", "a", "propX11", 1, "same", 123.01d, "extra", "something"));
        final Map<String, Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", "x");
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String, Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", "");
        final Map<String, Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", "y");
        properties.put("y", Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<VertexProperty> propertyX = StreamFactory.stream(dv.iterators().propertyIterator("x")).collect(Collectors.toList());
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
                p.label().equals(p.key())
                        && (p.id().equals(123) || p.id().equals(124))
                        && (p.value().equals("a") || p.value().equals("c"))));

        // there should be only one with properties on properties
        final VertexProperty propertyOnProperty = propertyX.stream().filter(p -> p.iterators().propertyIterator().hasNext()).findAny().get();
        assertEquals("a", propertyOnProperty.iterators().propertyIterator("propX1a").next().value());
        assertEquals(1, propertyOnProperty.iterators().propertyIterator("propX11").next().value());
        assertEquals(123.01d, propertyOnProperty.iterators().propertyIterator("same").next().value());
        assertEquals("something", propertyOnProperty.iterators().propertyIterator("extra").next().value());
        assertEquals(4, StreamFactory.stream(propertyOnProperty.iterators().propertyIterator()).count());
    }


    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowAddEdge() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);
        detachedVertex.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);
        detachedVertex.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);
        detachedVertex.remove();
    }

    @Test(expected = IllegalStateException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldNotBeAbleToCallPropertyIfThereAreMultipleProperties() {
        DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).property("location");
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeAbleToCallPropertyIfThereIsASingleProperty() {
        final DetachedVertex dv = DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true);
        assertEquals("marko", dv.property("name").value());
        assertEquals(29, dv.property("age").value());
    }
}
