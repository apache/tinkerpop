package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram {

    protected final Map<String, KeyType> computeKeys = new HashMap<String, KeyType>();
    private VertexQueryBuilder outgoingQuery = new VertexQueryBuilder().direction(Direction.OUT);
    private VertexQueryBuilder incomingQuery = new VertexQueryBuilder().direction(Direction.IN);

    public static final String PAGE_RANK = PageRankVertexProgram.class.getName() + ".pageRank";
    public static final String EDGE_COUNT = PageRankVertexProgram.class.getName() + ".edgeCount";

    private double vertexCountAsDouble = 1;
    private double alpha = 0.85d;
    private int totalIterations = 30;

    protected PageRankVertexProgram() {
        computeKeys.put(PAGE_RANK, VertexProgram.KeyType.VARIABLE);
        computeKeys.put(EDGE_COUNT, VertexProgram.KeyType.CONSTANT);
    }


    public Map<String, KeyType> getComputeKeys() {
        return computeKeys;
    }

    public void setup(final GraphMemory graphMemory) {

    }

    public void execute(final Vertex vertex, final GraphMemory graphMemory) {
        if (graphMemory.isInitialIteration()) {
            vertex.setProperty(PAGE_RANK, 1.0d / this.vertexCountAsDouble);
            vertex.setProperty(EDGE_COUNT, (double) this.outgoingQuery.build(vertex).count());
        } else {
            double newPageRank = 0.0d;
            for (final Vertex adjacent : this.incomingQuery.build(vertex).vertices()) {
                newPageRank += adjacent.<Double>getValue(PAGE_RANK) / adjacent.<Double>getValue(EDGE_COUNT);
            }
            vertex.setProperty(PAGE_RANK, (this.alpha * newPageRank) + ((1.0d - this.alpha) / this.vertexCountAsDouble));
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

        public Builder outgoing(final VertexQueryBuilder outgoingQuery) {
            this.vertexProgram.outgoingQuery = outgoingQuery;
            return this;
        }

        public Builder incoming(final VertexQueryBuilder incomingQuery) {
            this.vertexProgram.incomingQuery = incomingQuery;
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