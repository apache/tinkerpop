package com.tinkerpop.gremlin.process.computer.ranking;

import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram<Double> {

    protected final Map<String, KeyType> computeKeys = new HashMap<>();
    private MessageType.Local messageType = MessageType.Local.of(new VertexQueryBuilder().direction(Direction.OUT));

    public static final String PAGE_RANK = PageRankVertexProgram.class.getName() + ".pageRank";
    public static final String EDGE_COUNT = PageRankVertexProgram.class.getName() + ".edgeCount";

    private double vertexCountAsDouble = 1;
    private double alpha = 0.85d;
    private int totalIterations = 30;
    private boolean weighted = false;

    private PageRankVertexProgram() {

    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(PAGE_RANK, KeyType.VARIABLE, EDGE_COUNT, KeyType.CONSTANT);
    }

    public Class<Double> getMessageClass() {
        return Double.class;
    }

    public void setup(final Configuration configuration, final Graph.Memory.Computer graphMemory) {

    }

    public void execute(final Vertex vertex, Messenger<Double> messenger, final Graph.Memory.Computer graphMemory) {
        if (graphMemory.isInitialIteration()) {
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

    public boolean terminate(final Graph.Memory.Computer graphMemory) {
        return graphMemory.getIteration() >= this.totalIterations;
    }

    public static Builder create() {
        return new Builder();
    }

    //////////////////////////////

    public static class Builder {

        private final PageRankVertexProgram vertexProgram = new PageRankVertexProgram();

        public Builder iterations(final int iterations) {
            this.vertexProgram.totalIterations = iterations;
            return this;
        }

        public Builder alpha(final double alpha) {
            this.vertexProgram.alpha = alpha;
            return this;
        }

        public Builder messageType(final MessageType.Local messageType) {
            this.vertexProgram.messageType = messageType;
            return this;
        }

        public Builder weighted(final boolean weighted) {
            this.vertexProgram.weighted = weighted;
            return this;
        }

        public Builder vertexCount(final long count) {
            this.vertexProgram.vertexCountAsDouble = (double) count;
            return this;
        }

        public PageRankVertexProgram build() {
            return this.vertexProgram;
        }
    }

}