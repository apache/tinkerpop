package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IoMetaProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedVertex.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        DetachedVertex.detach(DetachedVertex.detach(v));
    }

    @Test
    @org.junit.Ignore
    public void shouldConstructDetachedVertex() {
        //assertEquals("1", this.detachedVertex.id());
        //assertEquals("l", this.detachedVertex.label());
    }

    @Test
    @org.junit.Ignore
    public void shouldEvaluateToEqual() {
        // assertTrue(detachedVertex1.equals(this.detachedVertex));
    }

    @Test
    @org.junit.Ignore
    public void shouldNotEvaluateToEqualDifferentId() {
        //assertFalse(detachedVertex1.equals(this.detachedVertex));
    }

    @Test
    public void shouldConstructDetachedVertexFromParts() {
        final Map<String,Object> properties = new HashMap<>();
        final Map<String,Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", MetaProperty.DEFAULT_LABEL);
        final Map<String,Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", MetaProperty.DEFAULT_LABEL);
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String,Object> hiddens = new HashMap<>();
        final Map<String,Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", MetaProperty.DEFAULT_LABEL);
        final Map<String,Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", MetaProperty.DEFAULT_LABEL);
        hiddens.put(Graph.Key.hide("y"), Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties, hiddens);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<MetaProperty> propertyX = StreamFactory.stream(dv.iterators().properties("x")).collect(Collectors.toList());
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
            p.label().equals(MetaProperty.DEFAULT_LABEL)
                    && (p.id().equals(123) || p.id().equals(124))
                    && (p.value().equals("a") || p.value().equals("c"))
                    && !p.iterators().properties().hasNext()
                    && !p.iterators().hiddens().hasNext()));

        final List<MetaProperty> propertyY = StreamFactory.stream(dv.iterators().hiddens("y")).collect(Collectors.toList());
        assertEquals(2, propertyY.size());
        assertTrue(propertyY.stream().allMatch(p ->
                p.label().equals(MetaProperty.DEFAULT_LABEL)
                        && (p.id().equals(125) || p.id().equals(126))
                        && (p.value().equals("b") || p.value().equals("d"))
                        && !p.iterators().properties().hasNext()
                        && !p.iterators().hiddens().hasNext()));
    }

    @Test
    public void shouldConstructDetachedVertexFromPartsWithPropertiesOnProperties() {
        final Map<String,Object> properties = new HashMap<>();
        final Map<String,Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", MetaProperty.DEFAULT_LABEL);
        propX1.put("properties", ElementHelper.asMap("propX1a", "a", "propX11", 1, "same", 123.01d, "extra", "something"));
        propX1.put("hidden", ElementHelper.asMap(Graph.Key.hide("propX1ha"), "ha", Graph.Key.hide("propX1h1"), 11, Graph.Key.hide("same"), 321.01d));
        final Map<String,Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", MetaProperty.DEFAULT_LABEL);
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String,Object> hiddens = new HashMap<>();
        final Map<String,Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", MetaProperty.DEFAULT_LABEL);
        final Map<String,Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", MetaProperty.DEFAULT_LABEL);
        hiddens.put(Graph.Key.hide("y"), Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties, hiddens);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<MetaProperty> propertyX = StreamFactory.stream(dv.iterators().properties("x")).collect(Collectors.toList());
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
                p.label().equals(MetaProperty.DEFAULT_LABEL)
                        && (p.id().equals(123) || p.id().equals(124))
                        && (p.value().equals("a") || p.value().equals("c"))));

        // there should be only one with properties on properties
        final MetaProperty propertyOnProperty = propertyX.stream().filter(p -> p.iterators().properties().hasNext()).findFirst().get();
        assertEquals("a", propertyOnProperty.iterators().properties("propX1a").next().value());
        assertEquals(1, propertyOnProperty.iterators().properties("propX11").next().value());
        assertEquals(123.01d, propertyOnProperty.iterators().properties("same").next().value());
        assertEquals("something", propertyOnProperty.iterators().properties("extra").next().value());
        assertEquals(4, StreamFactory.stream(propertyOnProperty.iterators().properties()).count());
        assertEquals("ha", propertyOnProperty.iterators().hiddens("propX1ha").next().value());
        assertEquals(11, propertyOnProperty.iterators().hiddens("propX1h1").next().value());
        assertEquals(321.01d, propertyOnProperty.iterators().hiddens("same").next().value());
        assertEquals(3, StreamFactory.stream(propertyOnProperty.iterators().hiddens()).count());

        final List<MetaProperty> propertyY = StreamFactory.stream(dv.iterators().hiddens("y")).collect(Collectors.toList());
        assertEquals(2, propertyY.size());
        assertTrue(propertyY.stream().allMatch(p ->
                p.label().equals(MetaProperty.DEFAULT_LABEL)
                        && (p.id().equals(125) || p.id().equals(126))
                        && (p.value().equals("b") || p.value().equals("d"))
                        && !p.iterators().properties().hasNext()
                        && !p.iterators().hiddens().hasNext()));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowAddEdge() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotTraverse() {
        final Vertex v = g.addVertex();
        final DetachedVertex detachedVertex = DetachedVertex.detach(v);
        detachedVertex.start();
    }

    @Test(expected = IllegalStateException.class)
    @org.junit.Ignore
    public void shouldNotBeAbleToCallPropertyIfThereAreMultipleProperties() {
        /*
        final Map<String,Object> properties = new HashMap<>();
        final IoMetaProperty propX1 = new IoMetaProperty();
        propX1.value = "a";
        propX1.id = 123;
        propX1.label = MetaProperty.DEFAULT_LABEL;
        final IoMetaProperty propX2 = new IoMetaProperty();
        propX2.value = "c";
        propX2.id = 124;
        propX2.label = MetaProperty.DEFAULT_LABEL;
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String,Object> hiddens = new HashMap<>();
        final DetachedVertex dv = new DetachedVertex(1, "test", properties, hiddens);
        dv.property("x");
        */
    }

    @Test
    @org.junit.Ignore
    public void shouldBeAbleToCallPropertyIfThereIsASingleProperty() {
        /*
        final Map<String,Object> properties = new HashMap<>();
        final IoMetaProperty propX1 = new IoMetaProperty();
        propX1.value = "a";
        propX1.id = 123;
        propX1.label = MetaProperty.DEFAULT_LABEL;
        properties.put("x", Arrays.asList(propX1));

        final Map<String,Object> hiddens = new HashMap<>();
        final DetachedVertex dv = new DetachedVertex(1, "test", properties, hiddens);
        assertEquals("a", dv.property("x").value());
        */
    }
}
