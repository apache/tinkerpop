package com.tinkerpop.gremlin.process.computer.ranking.pagerank;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankStep extends AbstractStep<Vertex, Pair<Vertex, Double>> {

    private final Graph graph;
    private boolean firstNext = true;
    private Graph resultantGraph;
    public double alpha;
    public Supplier<Traversal<Vertex, Edge>> incidentTraversal;

    public PageRankStep(final Traversal traversal, final double alpha) {
        super(traversal);
        this.graph = traversal.asAdmin().getSideEffects().getGraph();
        this.incidentTraversal = () -> this.graph.<Vertex>of().outE();
        this.alpha = alpha;
    }

    public PageRankStep(final Traversal traversal) {
        this(traversal, 0.85d);
    }

    public PageRankStep(final Traversal traversal, final Supplier<Traversal<Vertex, Edge>> incidentTraversal) {
        this(traversal, 0.85);
        this.incidentTraversal = incidentTraversal;
    }

    @Override
    public Traverser<Pair<Vertex, Double>> processNextStart() {
        try {
            if (this.firstNext) {
                this.resultantGraph = this.graph.compute().program(PageRankVertexProgram.build().alpha(this.alpha).incident(this.incidentTraversal).create()).submit().get().graph();
                this.firstNext = false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        final Traverser.Admin<Vertex> traverser = this.starts.next();
        final Vertex vertex = traverser.get();
        return traverser.split(this.getLabel(), new Pair<>(vertex, (Double) this.resultantGraph.V(vertex.id()).next().value(PageRankVertexProgram.PAGE_RANK)));
    }
}
