package com.tinkerpop.gremlin.process.computer.ranking.pagerank;

import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.marker.CountTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram<Double> {

    private MessageType.Local<?, ?> messageType = MessageType.Local.of(new OutETraversalSupplier());

    public static final String PAGE_RANK = Graph.Key.hide("gremlin.pageRankVertexProgram.pageRank");
    public static final String EDGE_COUNT = Graph.Key.hide("gremlin.pageRankVertexProgram.edgeCount");

    private static final String VERTEX_COUNT = "gremlin.pageRankVertexProgram.vertexCount";
    private static final String ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String TOTAL_ITERATIONS = "gremlin.pageRankVertexProgram.totalIterations";
    private static final String INCIDENT_TRAVERSAL_SUPPLIER = "gremlin.pageRankVertexProgram.incidentTraversalSupplier";

    private LambdaHolder<Supplier<CountTraversal<Vertex, Edge>>> traversalSupplier;
    private double vertexCountAsDouble = 1;
    private double alpha = 0.85d;
    private int totalIterations = 30;

    private static final Set<String> COMPUTE_KEYS = new HashSet<>(Arrays.asList(PAGE_RANK, EDGE_COUNT));

    private PageRankVertexProgram() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversalSupplier = LambdaHolder.loadState(configuration, INCIDENT_TRAVERSAL_SUPPLIER);
        if (null != this.traversalSupplier) {
            VertexProgramHelper.verifyReversibility(this.traversalSupplier.get().get());
            this.messageType = MessageType.Local.of(this.traversalSupplier.get());
        }
        this.vertexCountAsDouble = configuration.getDouble(VERTEX_COUNT, 1.0d);
        this.alpha = configuration.getDouble(ALPHA, 0.85d);
        this.totalIterations = configuration.getInt(TOTAL_ITERATIONS, 30);
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, PageRankVertexProgram.class.getName());
        configuration.setProperty(VERTEX_COUNT, this.vertexCountAsDouble);
        configuration.setProperty(ALPHA, this.alpha);
        configuration.setProperty(TOTAL_ITERATIONS, this.totalIterations);
        if (null != this.traversalSupplier) {
            this.traversalSupplier.storeState(configuration);
        }
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void execute(final Vertex vertex, Messenger<Double> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            double initialPageRank = 1.0d / this.vertexCountAsDouble;
            double edgeCount = Double.valueOf(this.messageType.<CountTraversal<Vertex, Edge>>edges(vertex).count().next());
            vertex.singleProperty(PAGE_RANK, initialPageRank);
            vertex.singleProperty(EDGE_COUNT, edgeCount);
            messenger.sendMessage(this.messageType, initialPageRank / edgeCount);
        } else {
            double newPageRank = StreamFactory.stream(messenger.receiveMessages(this.messageType)).reduce(0.0d, (a, b) -> a + b);
            newPageRank = (this.alpha * newPageRank) + ((1.0d - this.alpha) / this.vertexCountAsDouble);
            vertex.singleProperty(PAGE_RANK, newPageRank);
            messenger.sendMessage(this.messageType, newPageRank / vertex.<Double>property(EDGE_COUNT).orElse(0.0d));
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() >= this.totalIterations;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "alpha=" + this.alpha + ",iterations=" + this.totalIterations);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(PageRankVertexProgram.class);
        }

        public Builder iterations(final int iterations) {
            this.configuration.setProperty(TOTAL_ITERATIONS, iterations);
            return this;
        }

        public Builder alpha(final double alpha) {
            this.configuration.setProperty(ALPHA, alpha);
            return this;
        }

        public Builder incident(final String scriptEngine, final String traversalScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, INCIDENT_TRAVERSAL_SUPPLIER, new String[]{scriptEngine, traversalScript});
            return this;
        }

        public Builder incident(final String traversalScript) {
            return incident(GREMLIN_GROOVY, traversalScript);
        }

        public Builder incident(final Supplier<CountTraversal<Vertex, Edge>> traversal) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, INCIDENT_TRAVERSAL_SUPPLIER, traversal);
            return this;
        }

        public Builder incident(final Class<Supplier<CountTraversal<Vertex, Edge>>> traversalClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, INCIDENT_TRAVERSAL_SUPPLIER, traversalClass);
            return this;
        }

        public Builder vertexCount(final long vertexCount) {
            this.configuration.setProperty(VERTEX_COUNT, (double) vertexCount);
            return this;
        }
    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageTypes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }


    ////////////////////////////

    public static class OutETraversalSupplier implements Supplier<CountTraversal<Vertex, Edge>> {
        public CountTraversal<Vertex, Edge> get() {
            return GraphTraversal.<Vertex>of().outE();
        }
    }

}