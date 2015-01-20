package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ComputerDataStrategyTest extends AbstractGremlinTest {

    @Test
    public void shouldFilterHiddenProperties() {
        final StrategyGraph sg = g.strategy(new ComputerDataStrategy(new HashSet<>(Arrays.asList("***hidden-guy"))));

        final Vertex v = sg.addVertex("***hidden-guy", "X", "not-hidden-guy", "Y");
        final Iterator<VertexProperty<String>> props = v.iterators().propertyIterator();
        final VertexProperty v1 = props.next();
        assertEquals("Y", v1.value());
        assertEquals("not-hidden-guy", v1.key());
        assertFalse(props.hasNext());

        final Iterator<String> values = v.iterators().valueIterator();
        assertEquals("Y", values.next());
        assertFalse(values.hasNext());
    }

    @Test
    public void shouldAccessHiddenProperties() {
        final StrategyGraph sg = g.strategy(new ComputerDataStrategy(new HashSet<>(Arrays.asList("***hidden-guy"))));

        final Vertex v = sg.addVertex("***hidden-guy", "X", "not-hidden-guy", "Y");
        final Iterator<VertexProperty<String>> props = v.iterators().propertyIterator("***hidden-guy");
        final VertexProperty<String> v1 = props.next();
        assertEquals("X", v1.value());
        assertEquals("***hidden-guy", v1.key());
        assertFalse(props.hasNext());

        final Iterator<String> values = v.iterators().valueIterator("***hidden-guy");
        assertEquals("X", values.next());
        assertFalse(values.hasNext());
    }

    @Test
    public void shouldHideHiddenKeys() {
        final StrategyGraph sg = g.strategy(new ComputerDataStrategy(new HashSet<>(Arrays.asList("***hidden-guy"))));

        final Vertex v = sg.addVertex("***hidden-guy", "X", "not-hidden-guy", "Y");
        final Set<String> keys = v.keys();
        assertTrue(keys.contains("not-hidden-guy"));
        assertFalse(keys.contains("***hidden-guy"));
    }
}
