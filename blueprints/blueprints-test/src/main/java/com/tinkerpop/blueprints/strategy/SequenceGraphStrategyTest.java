package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategyTest extends AbstractBlueprintsTest {
    private static final String partition = Property.Key.hidden("partition");

    public SequenceGraphStrategyTest() {
        super(Optional.of(new SequenceGraphStrategy(
                new PartitionGraphStrategy(partition, "A"),
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Object[]> getPreAddVertex() {
                        return (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working"));
                            return o.toArray();
                        };
                    }
                })));
    }

    @Test
    public void shouldAppendPartitionAndPropertyToVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").getValue());
        assertEquals("A", v.getProperty(partition).getValue());
        assertEquals("working", v.getProperty("anonymous").getValue());
    }
}
