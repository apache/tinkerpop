package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.graph.map.VertexStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionGraphStrategy implements GraphStrategy {

    private String writePartition;
    private final String partitionKey;
    private final Set<String> readPartitions = new HashSet<>();

    public PartitionGraphStrategy(final String partitionKey, final String partition) {
        this.writePartition = partition;
        this.addReadPartition(partition);
        this.partitionKey = partitionKey;
    }

    public String getWritePartition() {
        return writePartition;
    }

    public void setWritePartition(final String writePartition) {
        this.writePartition = writePartition;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public Set<String> getReadPartitions() {
        return Collections.unmodifiableSet(readPartitions);
    }

    public void removeReadPartition(final String readPartition) {
        this.readPartitions.remove(readPartition);
    }

    public void addReadPartition(final String readPartition) {
        this.readPartitions.add(readPartition);
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getVStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> {
            final GraphTraversal traversal = f.get();
            traversal.optimizers().register(new PartitionGraphTraversalStrategy(this.partitionKey, this.readPartitions, ctx.getCurrent()));
            return traversal;
        };
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Edge, Edge>>> getEStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> {
            final GraphTraversal traversal = f.get();
            traversal.optimizers().register(new PartitionGraphTraversalStrategy(this.partitionKey, this.readPartitions, ctx.getCurrent()));
            return traversal;
        };
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(o.toArray());
        };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (label, v, keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(label, v, o.toArray());
        };
    }

	@Override
	public String toString() {
		return PartitionGraphStrategy.class.getSimpleName();
	}

    /**
     * Analyzes the traversal and injects the partition logic after every access to a vertex or edge.  The partition
     * logic consists of a {@link HasStep} with partition key and value followed by a {@link MapStep} that wraps
	 * the @{link Element} back up in a {@link StrategyWrapped} implementation.
     */
    public static class PartitionGraphTraversalStrategy implements TraversalStrategy.FinalTraversalStrategy {

        private final String partitionKey;
        private final Set<String> readPartitions;
        private final StrategyWrappedGraph graph;

        public PartitionGraphTraversalStrategy(final String partitionKey, final Set<String> readPartitions, final StrategyWrappedGraph graph) {
            this.partitionKey = partitionKey;
            this.readPartitions = readPartitions;
            this.graph = graph;
        }

        public void apply(final Traversal traversal) {
            // inject a HasStep and MapStep after each GraphStep, VertexStep or EdgeVertexStep
            final List<Class> stepsToLookFor = Arrays.<Class>asList(GraphStep.class, VertexStep.class, EdgeVertexStep.class);
            final List<Integer> positions = new ArrayList<>();
            final List<?> traversalSteps = traversal.getSteps();
            for (int ix = 0; ix < traversalSteps.size(); ix++) {
                final int pos = ix;
                if (stepsToLookFor.stream().anyMatch(c -> c.isAssignableFrom(traversalSteps.get(pos).getClass()))) positions.add(ix);
            }

            Collections.reverse(positions);
            for (int pos : positions) {
                final MapStep<Object, Object> transformToStrategy = new MapStep<>(traversal);
                transformToStrategy.setFunction((Holder<Object> t) -> {
                    // todo: need to make sure we're not re-wrapping in strategy over and over again.
                    final Object o = t.get();
                    if (o instanceof Vertex)
                        return new StrategyWrappedVertex((Vertex) o, graph);
                    else if (o instanceof Edge)
                        return new StrategyWrappedEdge((Edge) o, graph);
					else if (o instanceof Property)
						return new StrategyWrappedProperty((Property) o, graph);
                    else
                        return o;
                });

                TraversalHelper.insertStep(new HasStep(traversal, new HasContainer(this.partitionKey, T.convert(T.in), readPartitions)), pos + 1, traversal);
                TraversalHelper.insertStep(transformToStrategy, pos + 1, traversal);
            }
        }
    }
}