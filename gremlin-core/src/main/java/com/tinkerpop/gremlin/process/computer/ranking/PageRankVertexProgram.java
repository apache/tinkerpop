package com.tinkerpop.gremlin.process.computer.ranking;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram<Double> {

    private MessageType.Local messageType = MessageType.Local.of(() -> new DefaultGraphTraversal().outE());

    public static final String PAGE_RANK = Graph.Key.hidden("gremlin.pageRank");
    public static final String EDGE_COUNT = Graph.Key.hidden("gremlin.edgeCount");

    private static final String VERTEX_COUNT = "gremlin.pageRankVertexProgram.vertexCount";
    private static final String ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String TOTAL_ITERATIONS = "gremlin.pageRankVertexProgram.totalIterations";
    private static final String INCIDENT_TRAVERSAL = "gremlin.pageRankVertexProgram.incidentTraversal";
    private static final String DO_RESULT_MAP_REDUCE = "gremlin.pageRankVertexProgram.doResultMapReduce";

    private double vertexCountAsDouble = 1;
    private double alpha = 0.85d;
    private int totalIterations = 30;
    private boolean doResultMapReduce = false;

    public PageRankVertexProgram() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.vertexCountAsDouble = configuration.getDouble(VERTEX_COUNT, 1.0d);
        this.alpha = configuration.getDouble(ALPHA, 0.85d);
        this.totalIterations = configuration.getInt(TOTAL_ITERATIONS, 30);
        this.doResultMapReduce = configuration.getBoolean(DO_RESULT_MAP_REDUCE, false);
        try {
            if (configuration.containsKey(INCIDENT_TRAVERSAL)) {
                final SSupplier<Traversal> traversalSupplier = VertexProgramHelper.deserialize(configuration, INCIDENT_TRAVERSAL);
                VertexProgramHelper.verifyReversibility(traversalSupplier.get());
                this.messageType = MessageType.Local.of((SSupplier) traversalSupplier);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Map<String, KeyType> getElementKeys() {
        return VertexProgram.createElementKeys(PAGE_RANK, KeyType.VARIABLE, EDGE_COUNT, KeyType.CONSTANT);
    }

    @Override
    public Set<String> getSideEffectKeys() {
        return Collections.emptySet();
    }


    @Override
    public Class<Double> getMessageClass() {
        return Double.class;
    }

    @Override
    public void setup(final SideEffects sideEffects) {

    }

    @Override
    public List<MapReduce> getMapReducers() {
        return this.doResultMapReduce ? Arrays.asList(new PageRankMapReduce()) : Collections.emptyList();
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Double> messenger, final SideEffects sideEffects) {
        if (sideEffects.isInitialIteration()) {
            double initialPageRank = 1.0d / this.vertexCountAsDouble;
            double edgeCount = Double.valueOf((Long) this.messageType.edges(vertex).count().next());
            vertex.property(PAGE_RANK, initialPageRank);
            vertex.property(EDGE_COUNT, edgeCount);
            messenger.sendMessage(this.messageType, initialPageRank / edgeCount);
        } else {
            double newPageRank = StreamFactory.stream(messenger.receiveMessages(this.messageType)).reduce(0.0d, (a, b) -> a + b);
            newPageRank = (this.alpha * newPageRank) + ((1.0d - this.alpha) / this.vertexCountAsDouble);
            vertex.property(PAGE_RANK, newPageRank);
            messenger.sendMessage(this.messageType, newPageRank / vertex.<Double>property(EDGE_COUNT).orElse(0.0d));
        }
    }

    @Override
    public boolean terminate(final SideEffects sideEffects) {
        return sideEffects.getIteration() >= this.totalIterations;
    }

    //////////////////////////////

    public static Builder create() {
        return new Builder();
    }

    public static class Builder implements VertexProgram.Builder {

        private final Configuration configuration = new BaseConfiguration();

        private Builder() {
            this.configuration.setProperty(GraphComputer.VERTEX_PROGRAM, PageRankVertexProgram.class.getName());
        }

        public Builder iterations(final int iterations) {
            this.configuration.setProperty(TOTAL_ITERATIONS, iterations);
            return this;
        }

        public Builder alpha(final double alpha) {
            this.configuration.setProperty(ALPHA, alpha);
            return this;
        }

        public Builder incidentTraversal(final SSupplier<Traversal<Vertex, Edge>> incidentTraversal) throws IOException {
            try {
                VertexProgramHelper.serialize(incidentTraversal, this.configuration, INCIDENT_TRAVERSAL);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder vertexCount(final long vertexCount) {
            this.configuration.setProperty(VERTEX_COUNT, (double) vertexCount);
            return this;
        }

        public Configuration getConfiguration() {
            return this.configuration;
        }
    }

    ////////////////////////////

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

}