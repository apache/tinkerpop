package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionStrategyTest extends AbstractBlueprintsTest {
    private static final String partition = Property.Key.hidden("partition");

    public PartitionStrategyTest() {
        super(Optional.of(new PartitionGraphStrategy(partition, "A")));
    }

    @Test
    public void shouldAppendPartitionToVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").getValue());
        assertEquals("A", v.getProperty(partition).getValue());

        /*
        final GraphStrategy anonymousStrategy = new GraphStrategy() {
            @Override
            public Function<Object[], Object[]> getPreAddVertex() {
                return (args) -> {
                    final List<Object> o = new ArrayList<>(Arrays.asList(args));
                    o.addAll(Arrays.asList("anonymous", "working"));
                    return o.toArray();
                };
            }
        };

        final GraphStrategy sequenceStrategy = new GraphStrategy.SequenceGraphStrategy(partitionStrategy, anonymousStrategy);
        final Graph sequencedGraph = new StrategyGraph(g, sequenceStrategy);
        final Vertex x = sequencedGraph.addVertex("some", "thing");

        assertNotNull(v);
        assertEquals("thing", x.getProperty("some").getValue());
        assertEquals("A", x.getProperty(partition).getValue());
        assertEquals("working", x.getProperty("anonymous").getValue());
        */
    }

    @Test
    public void shouldAppendPartitionToEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(v1);
        assertEquals("thing", v1.getProperty("any").getValue());
        assertEquals("A", v2.getProperty(partition).getValue());

        assertNotNull(v2);
        assertEquals("thing", v2.getProperty("some").getValue());
        assertEquals("A", v2.getProperty(partition).getValue());

        assertNotNull(e);
        assertEquals("thing", e.getProperty("every").getValue());
        assertEquals("connectsTo", e.getLabel());
        assertEquals("A", e.getProperty(partition).getValue());
    }
}
