package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.TriFunction;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@link GraphStrategy} which enables support for logical graph partitioning where the Graph can be blinded to
 * different parts of the total {@link com.tinkerpop.gremlin.structure.Graph}.  Note that the {@code partitionKey}
 * is hidden by this strategy.  Use the base {@link com.tinkerpop.gremlin.structure.Graph} to access that.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PartitionStrategy extends SubgraphStrategy {

    private String writePartition;
    private final String partitionKey;
    private final Set<String> readPartitions = new HashSet<>();

    private PartitionStrategy(final String partitionKey, final String partition) {
        super(null, null);
        this.vertexPredicate = this::testElement;
        this.edgePredicate = this::testElement;

        this.writePartition = partition;
        this.addReadPartition(partition);
        this.partitionKey = partitionKey;
    }

    private boolean testElement(final Element e) {
        final Property<String> p = e.property(this.partitionKey);
        return p.isPresent() && this.readPartitions.contains(p.value());
    }

    public String getWritePartition() {
        return this.writePartition;
    }

    public void setWritePartition(final String writePartition) {
        this.writePartition = writePartition;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public Set<String> getReadPartitions() {
        return Collections.unmodifiableSet(this.readPartitions);
    }

    public void removeReadPartition(final String readPartition) {
        this.readPartitions.remove(readPartition);
    }

    public void addReadPartition(final String readPartition) {
        this.readPartitions.add(readPartition);
    }

    public void clearReadPartitions() {
        this.readPartitions.clear();
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertex> ctx) {
        return (f) -> (keys) -> IteratorUtils.filter(f.apply(keys), property -> !partitionKey.equals(property.key()));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertex> ctx) {
        return (f) -> (keys) -> IteratorUtils.map(ctx.getCurrent().iterators().<V>propertyIterator(keys), vertexProperty -> vertexProperty.value());
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final StrategyContext<StrategyVertex> ctx) {
        return (f) -> () -> IteratorUtils.fill(IteratorUtils.filter(f.get().iterator(), key -> !partitionKey.equals(key)), new HashSet<>());
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<Property<V>>>> getEdgeIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyEdge> ctx) {
        return (f) -> (keys) -> IteratorUtils.filter(f.apply(keys), property -> !partitionKey.equals(property.key()));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsValueIteratorStrategy(final StrategyContext<StrategyEdge> ctx) {
        return (f) -> (keys) -> IteratorUtils.map(ctx.getCurrent().iterators().<V>propertyIterator(keys), property -> property.value());
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final StrategyContext<StrategyEdge> ctx) {
        return (f) -> () -> IteratorUtils.fill(IteratorUtils.filter(f.get().iterator(), key -> !partitionKey.equals(key)), new HashSet<>());
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final StrategyContext<StrategyGraph> ctx) {
        return (f) -> (keyValues) -> f.apply(this.addKeyValues(keyValues));
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final StrategyContext<StrategyVertex> ctx) {
        return (f) -> (label, v, keyValues) -> f.apply(label, v, this.addKeyValues(keyValues));
    }

    private final Object[] addKeyValues(final Object[] keyValues) {
        final Object[] keyValuesExtended = Arrays.copyOf(keyValues, keyValues.length + 2);
        keyValuesExtended[keyValues.length] = this.partitionKey;
        keyValuesExtended[keyValues.length + 1] = this.writePartition;
        return keyValuesExtended;
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private String startPartition = "default";
        private String partitionKey = "_partition" ;

        private Builder() {}

        /**
         * The initial partition to filter by. If this value is not set, it will be defaulted to "default".
         */
        public Builder startPartition(final String startPartition) {
            if (null == startPartition) throw new IllegalArgumentException("The startPartition cannot be null");
            this.startPartition = startPartition;
            return this;
        }

        /**
         * The name of the partition key.  If this is not set, then the value is defaulted to "_partition".
         */
        public Builder partitionKey(final String partitionKey) {
            if (null == partitionKey) throw new IllegalArgumentException("The partitionKey cannot be null");
            this.partitionKey = partitionKey;
            return this;
        }

        public PartitionStrategy create() {
            return new PartitionStrategy(partitionKey, startPartition);
        }
    }
}