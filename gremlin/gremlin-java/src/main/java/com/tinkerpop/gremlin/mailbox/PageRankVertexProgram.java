package com.tinkerpop.gremlin.mailbox;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.mailbox.GraphMemory;
import com.tinkerpop.blueprints.mailbox.Mailbox;
import com.tinkerpop.blueprints.mailbox.VertexProgram;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram<Double> {

    protected final Map<String, KeyType> computeKeys = new HashMap<String, KeyType>();
    private VertexQueryBuilder adjacentQuery = new VertexQueryBuilder().direction(Direction.OUT);

    public static final String PAGE_RANK = PageRankVertexProgram.class.getName() + ".pageRank";
    public static final String EDGE_COUNT = PageRankVertexProgram.class.getName() + ".edgeCount";

    private double vertexCountAsDouble = 1;
    private double alpha = 0.85d;
    private int totalIterations = 30;

    protected PageRankVertexProgram() {
        computeKeys.put(PAGE_RANK, VertexProgram.KeyType.VARIABLE);
        computeKeys.put(EDGE_COUNT, VertexProgram.KeyType.CONSTANT);
        computeKeys.put(Property.Key.hidden("mailbox"), KeyType.VARIABLE);
    }


    public Map<String, KeyType> getComputeKeys() {
        return computeKeys;
    }

    public void setup(final GraphMemory graphMemory) {

    }

    public void execute(final Vertex vertex, Mailbox<Double> mailbox, final GraphMemory graphMemory) {
        if (graphMemory.isInitialIteration()) {
            double newPageRank = 1.0d / this.vertexCountAsDouble;
            List<Object> ids = StreamFactory.stream(this.adjacentQuery.build(vertex).vertices()).map(Vertex::getId).collect(Collectors.toList());
            vertex.setProperty(PAGE_RANK, newPageRank);
            vertex.setProperty(EDGE_COUNT, ids.size());
            mailbox.sendMessage(vertex, ids, newPageRank / ids.size());
        } else {
            double newPageRank = 0.0d;
            for (final Double pageRank : mailbox.getMessages(vertex)) {
                newPageRank += pageRank;
            }
            newPageRank = (this.alpha * newPageRank) + ((1.0d - this.alpha) / this.vertexCountAsDouble);
            vertex.setProperty(PAGE_RANK, newPageRank);
            List<Object> ids = StreamFactory.stream(this.adjacentQuery.build(vertex).vertices()).map(Vertex::getId).collect(Collectors.toList());
            mailbox.sendMessage(vertex, ids, newPageRank / ((Integer) vertex.getValue(EDGE_COUNT)).doubleValue());
        }
    }

    public boolean terminate(final GraphMemory graphMemory) {
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

        public Builder adjacent(final VertexQueryBuilder adjacentQuery) {
            this.vertexProgram.adjacentQuery = adjacentQuery;
            return this;
        }

        public Builder vertexCount(final int count) {
            this.vertexProgram.vertexCountAsDouble = (double) count;
            return this;
        }

        public PageRankVertexProgram build() {
            return this.vertexProgram;
        }
    }

}