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
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedEdgeTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedEdge.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        DetachedEdge.detach(DetachedEdge.detach(e));
    }

    // todo: need "the crew"
    @Test
    @Ignore
    public void shouldConstructDetachedEdge() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("knows", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        assertEquals("3", detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertices(Direction.OUT).next().getClass());
        assertEquals("1", detachedEdge.iterators().vertices(Direction.OUT).next().id());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertices(Direction.IN).next().getClass());
        assertEquals("2", detachedEdge.iterators().vertices(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(detachedEdge.iterators()).count());
        assertEquals("a", detachedEdge.iterators().properties("x").next().value());
        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().properties("x")).count());
        assertEquals("b", detachedEdge.iterators().hiddens("y").next().value());
        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().hiddens("y")).count());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop"))).equals(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop")))));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop"))).hashCode(), DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop"))).hashCode());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex vOut = g.v(convertToVertexId("josh"));
        final Vertex vIn = g.v(convertToVertexId("lop"));
        final Edge e = vOut.addEdge("created", vIn, "weight", 0.4d);
        assertFalse(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop"))).equals(DetachedEdge.detach(e)));
    }

    @Test
    public void shouldConstructDetachedEdgeFromParts() {
        final Map<String,Object> properties = new HashMap<>();
        properties.put("x", "a");

        final Map<String,Object> hiddens = new HashMap<>();
        hiddens.put(Graph.Key.hide("y"), "b");

        final DetachedEdge de = new DetachedEdge(10, "bought", properties, hiddens, Pair.with(1, "person"), Pair.with(2, "product"));

        assertEquals(10, de.id());
        assertEquals("bought", de.label());
        assertEquals("person", de.iterators().vertices(Direction.OUT).next().label());
        assertEquals(1, de.iterators().vertices(Direction.OUT).next().id());
        assertEquals("product", de.iterators().vertices(Direction.IN).next().label());
        assertEquals(2, de.iterators().vertices(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(de.iterators()).count());
        assertEquals("a", de.iterators().properties("x").next().value());
        assertEquals(1, StreamFactory.stream(de.iterators().properties("x")).count());
        assertEquals("b", de.iterators().hiddens("y").next().value());
        assertEquals(1, StreamFactory.stream(de.iterators().hiddens("y")).count());

        assertEquals("a", de.property("x").value());
        assertEquals("x", de.property("x").key());
        assertFalse(de.property("x").isHidden());

        assertEquals("b", de.property(Graph.Key.hide("y")).value());
        assertEquals("y", de.property(Graph.Key.hide("y")).key());
        assertTrue(de.property(Graph.Key.hide("y")).isHidden());
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotTraverse() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.start();
    }
}
