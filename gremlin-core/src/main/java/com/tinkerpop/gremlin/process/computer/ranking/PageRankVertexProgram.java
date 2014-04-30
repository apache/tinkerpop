package com.tinkerpop.gremlin.process.computer.ranking;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram<Double> {

    private MessageType.Local messageType = MessageType.Local.of(new VertexQueryBuilder().direction(Direction.OUT));

    public static final String PAGE_RANK = "gremlin.pageRankVertexProgram.pageRank";
    public static final String EDGE_COUNT = "gremlin.pageRankVertexProgram.edgeCount";

    private static final String VERTEX_COUNT = "gremlin.pageRankVertexProgram.vertexCount";
    private static final String ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String TOTAL_ITERATIONS = "gremlin.pageRankVertexProgram.totalIterations";
    private static final String WEIGHTED = "gremlin.pageRankVertexProgram.weighted";

    private double vertexCountAsDouble = 1;
    private double alpha = 0.85d;
    private int totalIterations = 30;
    private boolean weighted = false;

    public PageRankVertexProgram() {

    }

    public void initialize(final Configuration configuration) {
        this.vertexCountAsDouble = configuration.getDouble(VERTEX_COUNT, 1.0d);
        this.alpha = configuration.getDouble(ALPHA, 0.85d);
        this.totalIterations = configuration.getInt(TOTAL_ITERATIONS, 30);
        this.weighted = configuration.getBoolean(WEIGHTED, false);
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(PAGE_RANK, KeyType.VARIABLE, EDGE_COUNT, KeyType.CONSTANT);
    }

    public Class<Double> getMessageClass() {
        return Double.class;
    }

    public void setup(final GraphComputer.Memory graphComputerMemory) {

    }

    public void execute(final Vertex vertex, Messenger<Double> messenger, final GraphComputer.Memory graphComputerMemory) {
        if (graphComputerMemory.isInitialIteration()) {
            double initialPageRank = 1.0d / this.vertexCountAsDouble;
            double edgeCount = Long.valueOf(this.messageType.getQuery().build(vertex).count()).doubleValue();
            vertex.setProperty(PAGE_RANK, initialPageRank);
            vertex.setProperty(EDGE_COUNT, edgeCount);
            if (this.weighted)
                messenger.sendMessage(vertex, this.messageType, initialPageRank);
            else
                messenger.sendMessage(vertex, this.messageType, initialPageRank / edgeCount);
        } else {
            double newPageRank = StreamFactory.stream(messenger.receiveMessages(vertex, this.messageType)).reduce(0.0d, (a, b) -> a + b);
            newPageRank = (this.alpha * newPageRank) + ((1.0d - this.alpha) / this.vertexCountAsDouble);
            vertex.setProperty(PAGE_RANK, newPageRank);
            if (this.weighted)
                messenger.sendMessage(vertex, this.messageType, newPageRank);
            else
                messenger.sendMessage(vertex, this.messageType, newPageRank / vertex.<Double>getProperty(EDGE_COUNT).orElse(0.0d));
        }
    }

    public boolean terminate(final GraphComputer.Memory graphComputerMemory) {
        return graphComputerMemory.getIteration() >= this.totalIterations;
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

        public Builder messageType(final MessageType.Local messageType) {
            //this.vertexProgram.messageType = messageType;
            return this;
        }

        public Builder weighted(final boolean weighted) {
            this.configuration.setProperty(WEIGHTED, weighted);
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

            public boolean requiresLocalMessageTypes() {
                return true;
            }

            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

}