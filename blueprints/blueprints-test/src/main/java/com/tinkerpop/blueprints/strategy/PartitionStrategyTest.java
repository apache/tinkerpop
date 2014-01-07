package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionStrategyTest extends AbstractBlueprintsTest {

    public PartitionStrategyTest() {
        super(Optional.of(new PartitionStrategy(Property.Key.hidden("partition"), "A")));
    }

    @Test
    public void shouldAppendPartitionToVertex() {
        final String partition = Property.Key.hidden("partition");
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").getValue());
        assertEquals("A", v.getProperty(partition).getValue());

        /*
        final Strategy anonymousStrategy = new Strategy() {
            @Override
            public Function<Object[], Object[]> getPreAddVertex() {
                return (args) -> {
                    final List<Object> o = new ArrayList<>(Arrays.asList(args));
                    o.addAll(Arrays.asList("anonymous", "working"));
                    return o.toArray();
                };
            }
        };

        final Strategy sequenceStrategy = new Strategy.SequenceStrategy(partitionStrategy, anonymousStrategy);
        final Graph sequencedGraph = new StrategyGraph(g, sequenceStrategy);
        final Vertex x = sequencedGraph.addVertex("some", "thing");

        assertNotNull(v);
        assertEquals("thing", x.getProperty("some").getValue());
        assertEquals("A", x.getProperty(partition).getValue());
        assertEquals("working", x.getProperty("anonymous").getValue());
        */
    }
}
