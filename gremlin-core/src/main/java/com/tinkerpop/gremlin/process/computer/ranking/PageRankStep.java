package com.tinkerpop.gremlin.process.computer.ranking;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.javatuples.Pair;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankStep extends AbstractStep<Vertex, Pair<Vertex, Double>> {

    private final Graph graph;
    private boolean firstNext = true;
    private Graph resultantGraph;
    public double alpha;
    public SSupplier<Traversal<Vertex, Edge>> incidentTraversal = () -> GraphTraversal.of().outE();

    public PageRankStep(final Traversal traversal, final double alpha) {
        super(traversal);
        this.graph = traversal.memory().get(Property.hidden("g"));
        this.alpha = alpha;
    }

    public PageRankStep(final Traversal traversal) {
        this(traversal, 0.85d);
    }

    public PageRankStep(final Traversal traversal, final SSupplier<Traversal<Vertex, Edge>> incidentTraversal) {
        this(traversal, 0.85);
        this.incidentTraversal = incidentTraversal;
    }

    public Traverser<Pair<Vertex, Double>> processNextStart() {
        try {
            if (this.firstNext) {
                this.resultantGraph = this.graph.compute().program(PageRankVertexProgram.create().alpha(this.alpha).incidentTraversal(this.incidentTraversal).getConfiguration()).submit().get().getValue0();
                this.firstNext = false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        final Traverser<Vertex> traverser = this.starts.next();
        final Vertex vertex = traverser.get();
        return traverser.makeChild(this.getAs(), new Pair<>(vertex, (Double) this.resultantGraph.v(vertex.id()).value(PageRankVertexProgram.PAGE_RANK)));
    }
}
