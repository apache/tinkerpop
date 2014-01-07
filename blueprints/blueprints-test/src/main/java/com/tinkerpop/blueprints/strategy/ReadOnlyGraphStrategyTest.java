package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategyTest extends AbstractBlueprintsTest {
    public ReadOnlyGraphStrategyTest() {
        super(Optional.of(new ReadOnlyGraphStrategy()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemoveVertex() {
        final Vertex v = g.addVertex();
        v.remove();
    }
}
