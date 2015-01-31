package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedEdgeTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge de = DetachedFactory.detach(e, false);
        assertSame(de, DetachedFactory.detach(de, false));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructDetachedEdge() {
        g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("year", 2002);
        final DetachedEdge detachedEdge = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), true);
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());

        assertEquals(2, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator("year")).count());
        assertEquals(0.5d, detachedEdge.iterators().propertyIterator("weight").next().value());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructDetachedEdgeAsReference() {
        g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("year", 2002);
        final DetachedEdge detachedEdge = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), false);
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());

        assertEquals(0, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true)));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).hashCode(), DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).hashCode());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Edge toDetach = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(toDetach, true);
        final Edge attached = detachedEdge.attach(g);

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedEdge);
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Edge toDetach = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Vertex outV = toDetach.iterators().vertexIterator(Direction.OUT).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(toDetach, true);
        final Edge attached = detachedEdge.attach(outV);

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedEdge);
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Vertex vOut = g.V(convertToVertexId("josh")).next();
        final Vertex vIn = g.V(convertToVertexId("lop")).next();
        final Edge e = vOut.addEdge("created", vIn, "weight", 0.4d);
        assertFalse(DetachedFactory.detach(g.E(joshCreatedLopEdgeId).next(), true).equals(DetachedFactory.detach(e, true)));
    }

    @Test
    public void shouldConstructDetachedEdgeFromParts() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("x", "a");
        properties.put("y", "b");

        final DetachedEdge de = new DetachedEdge(10, "bought", properties, Pair.with(1, "person"), Pair.with(2, "product"));

        assertEquals(10, de.id());
        assertEquals("bought", de.label());
        assertEquals("person", de.iterators().vertexIterator(Direction.OUT).next().label());
        assertEquals(1, de.iterators().vertexIterator(Direction.OUT).next().id());
        assertEquals("product", de.iterators().vertexIterator(Direction.IN).next().label());
        assertEquals(2, de.iterators().vertexIterator(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(de.iterators()).count());
        assertEquals("a", de.iterators().propertyIterator("x").next().value());
        assertEquals(1, StreamFactory.stream(de.iterators().propertyIterator("x")).count());

        assertEquals("a", de.property("x").value());
        assertEquals("x", de.property("x").key());

        assertEquals("b", de.property("y").value());
        assertEquals("y", de.property("y").key());
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedFactory.detach(e, false);
        detachedEdge.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedFactory.detach(e, false);
        detachedEdge.remove();
    }
}
