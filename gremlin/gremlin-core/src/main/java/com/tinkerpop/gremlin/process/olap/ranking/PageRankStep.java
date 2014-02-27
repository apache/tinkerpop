package com.tinkerpop.gremlin.process.olap.ranking;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankStep extends AbstractStep<Vertex, Pair<Vertex, Double>> {

    private final Graph graph;
    private boolean firstNext = true;
    private Graph resultantGraph;
    public double alpha;

    public PageRankStep(final Traversal traversal, final double alpha) {
        super(traversal);
        this.graph = traversal.memory().get(Traversal.Memory.Variable.hidden("g"));
        this.alpha = alpha;
    }

    public PageRankStep(final Traversal traversal) {
        this(traversal, 0.85d);
    }

    public Holder<Pair<Vertex, Double>> processNextStart() {
        try {
            if (this.firstNext) {
                this.resultantGraph = this.graph.compute().program(PageRankVertexProgram.create().alpha(this.alpha).build()).submit().get();
                this.firstNext = false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        final Holder<Vertex> holder = this.starts.next();
        final Vertex vertex = holder.get();
        return holder.makeChild(this.getAs(), new Pair<>(vertex, (Double) this.resultantGraph.v(vertex.getId()).getValue(PageRankVertexProgram.PAGE_RANK)));
    }
}
