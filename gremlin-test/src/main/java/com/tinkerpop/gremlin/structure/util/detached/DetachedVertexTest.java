package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedVertex.detach(null);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final DetachedVertex dv = DetachedVertex.detach(v);
        assertSame(dv, DetachedVertex.detach(dv));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructDetachedVertex() {
        final Vertex v = g.addVertex("test", "123", Graph.Key.hide("test"), "321");
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);

        assertEquals(v.id(), detachedVertex.id());
        assertEquals(v.label(), detachedVertex.label());
        assertEquals("123", detachedVertex.value("test"));
        assertEquals("321", detachedVertex.iterators().hiddenPropertyIterator("test").next().value().toString());
        assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
        assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldDetachVertexWithMultiPropertiesAndMetaProperties() {
        final DetachedVertex v1 = DetachedVertex.detach(convertToVertex(g, "marko"));

        assertEquals("person", v1.label());
        assertEquals(true, v1.iterators().hiddenValueIterator("visible").next());
        assertEquals(2, v1.keys().size());
        assertEquals(1, v1.hiddenKeys().size());
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
        assertTrue(DetachedVertex.detach(g.v(convertToVertexId("marko"))).equals(DetachedVertex.detach(g.v(convertToVertexId("marko")))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedVertex.detach(g.v(convertToVertexId("marko"))).hashCode(), DetachedVertex.detach(g.v(convertToVertexId("marko"))).hashCode());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v = g.addVertex("name", "marko", "age", 29);
        assertFalse(DetachedVertex.detach(v).equals(DetachedVertex.detach(g.v(convertToVertexId("marko")))));
    }

    @Test
    public void shouldConstructDetachedVertexFromParts() {
        final Map<String,Object> properties = new HashMap<>();
        final Map<String,Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", VertexProperty.DEFAULT_LABEL);
        final Map<String,Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", VertexProperty.DEFAULT_LABEL);
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String,Object> hiddens = new HashMap<>();
        final Map<String,Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", VertexProperty.DEFAULT_LABEL);
        final Map<String,Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", VertexProperty.DEFAULT_LABEL);
        hiddens.put(Graph.Key.hide("y"), Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties, hiddens);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<VertexProperty> propertyX = StreamFactory.stream(dv.iterators().propertyIterator("x")).collect(Collectors.toList());
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
            p.label().equals(VertexProperty.DEFAULT_LABEL)
                    && (p.id().equals(123) || p.id().equals(124))
                    && (p.value().equals("a") || p.value().equals("c"))
                    && !p.iterators().propertyIterator().hasNext()
                    && !p.iterators().hiddenPropertyIterator().hasNext()));

        final List<VertexProperty> propertyY = StreamFactory.stream(dv.iterators().hiddenPropertyIterator("y")).collect(Collectors.toList());
        assertEquals(2, propertyY.size());
        assertTrue(propertyY.stream().allMatch(p ->
                p.label().equals(VertexProperty.DEFAULT_LABEL)
                        && (p.id().equals(125) || p.id().equals(126))
                        && (p.value().equals("b") || p.value().equals("d"))
                        && !p.iterators().propertyIterator().hasNext()
                        && !p.iterators().hiddenPropertyIterator().hasNext()));
    }

    @Test
    public void shouldConstructDetachedVertexFromPartsWithPropertiesOnProperties() {
        final Map<String,Object> properties = new HashMap<>();
        final Map<String,Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", VertexProperty.DEFAULT_LABEL);
        propX1.put("properties", ElementHelper.asMap("propX1a", "a", "propX11", 1, "same", 123.01d, "extra", "something"));
        propX1.put("hidden", ElementHelper.asMap(Graph.Key.hide("propX1ha"), "ha", Graph.Key.hide("propX1h1"), 11, Graph.Key.hide("same"), 321.01d));
        final Map<String,Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", VertexProperty.DEFAULT_LABEL);
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String,Object> hiddens = new HashMap<>();
        final Map<String,Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", VertexProperty.DEFAULT_LABEL);
        final Map<String,Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", VertexProperty.DEFAULT_LABEL);
        hiddens.put(Graph.Key.hide("y"), Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties, hiddens);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<VertexProperty> propertyX = StreamFactory.stream(dv.iterators().propertyIterator("x")).collect(Collectors.toList());
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
                p.label().equals(VertexProperty.DEFAULT_LABEL)
                        && (p.id().equals(123) || p.id().equals(124))
                        && (p.value().equals("a") || p.value().equals("c"))));

        // there should be only one with properties on properties
        final VertexProperty propertyOnProperty = propertyX.stream().filter(p -> p.iterators().propertyIterator().hasNext()).findAny().get();
        assertEquals("a", propertyOnProperty.iterators().propertyIterator("propX1a").next().value());
        assertEquals(1, propertyOnProperty.iterators().propertyIterator("propX11").next().value());
        assertEquals(123.01d, propertyOnProperty.iterators().propertyIterator("same").next().value());
        assertEquals("something", propertyOnProperty.iterators().propertyIterator("extra").next().value());
        assertEquals(4, StreamFactory.stream(propertyOnProperty.iterators().propertyIterator()).count());
        assertEquals("ha", propertyOnProperty.iterators().hiddenPropertyIterator("propX1ha").next().value());
        assertEquals(11, propertyOnProperty.iterators().hiddenPropertyIterator("propX1h1").next().value());
        assertEquals(321.01d, propertyOnProperty.iterators().hiddenPropertyIterator("same").next().value());
        assertEquals(3, StreamFactory.stream(propertyOnProperty.iterators().hiddenPropertyIterator()).count());

        final List<VertexProperty> propertyY = StreamFactory.stream(dv.iterators().hiddenPropertyIterator("y")).collect(Collectors.toList());
        assertEquals(2, propertyY.size());
        assertTrue(propertyY.stream().allMatch(p ->
                p.label().equals(VertexProperty.DEFAULT_LABEL)
                        && (p.id().equals(125) || p.id().equals(126))
                        && (p.value().equals("b") || p.value().equals("d"))
                        && !p.iterators().propertyIterator().hasNext()
                        && !p.iterators().hiddenPropertyIterator().hasNext()));
    }


    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowAddEdge() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotTraverse() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.start();
    }

    @Test(expected = IllegalStateException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldNotBeAbleToCallPropertyIfThereAreMultipleProperties() {
        DetachedVertex.detach(g.v(convertToVertexId("marko"))).property("location");
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeAbleToCallPropertyIfThereIsASingleProperty() {
        final DetachedVertex dv = DetachedVertex.detach(g.v(convertToVertexId("marko")));
        assertEquals("marko", dv.property("name").value());
        assertEquals(29, dv.property("age").value());
    }
}
