package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IoMetaProperty;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexTest {

    private DetachedVertex detachedVertex;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        this.detachedVertex = DetachedVertex.detach(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedVertex.detach(null);
    }

    @Test
    public void shouldConstructDetachedVertex() {
        assertEquals("1", this.detachedVertex.id());
        assertEquals("l", this.detachedVertex.label());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        final DetachedVertex detachedVertex1 = DetachedVertex.detach(v);
        assertTrue(detachedVertex1.equals(this.detachedVertex));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("2");
        when(v.label()).thenReturn("l");

        final DetachedVertex detachedVertex1 = DetachedVertex.detach(v);
        assertFalse(detachedVertex1.equals(this.detachedVertex));
    }

    @Test
    public void shouldConstructDetachedVertexFromParts() {
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
        final IoMetaProperty propY1 = new IoMetaProperty();
        propY1.value = "b";
        propY1.id = 125;
        propY1.label = MetaProperty.DEFAULT_LABEL;
        final IoMetaProperty propY2 = new IoMetaProperty();
        propY2.value = "d";
        propY2.id = 126;
        propY2.label = MetaProperty.DEFAULT_LABEL;
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

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowAddEdge() {
        this.detachedVertex.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        this.detachedVertex.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.detachedVertex.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotTraverse() {
        this.detachedVertex.start();
    }
}
