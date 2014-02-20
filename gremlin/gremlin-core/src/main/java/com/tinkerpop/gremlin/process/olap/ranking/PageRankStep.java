package com.tinkerpop.gremlin.process.olap.ranking;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.ComputeResult;
import com.tinkerpop.gremlin.process.steps.AbstractStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankStep extends AbstractStep<Vertex, Pair<Vertex, Double>> {

    private final Graph graph;
    private boolean firstCall = true;
    private ComputeResult result;

    public PageRankStep(final Traversal traversal, final Graph graph) {
        super(traversal);
        this.graph = graph;
    }

    public Holder<Pair<Vertex, Double>> processNextStart() {
        try {
            if (this.firstCall) {
                this.result = this.graph.compute().program(PageRankVertexProgram.create().build()).submit().get();
                this.firstCall = false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        final Holder<Vertex> holder = this.starts.next();
        final Vertex vertex = holder.get();
        return holder.makeChild(this.getAs(), new Pair<>(vertex, (Double) this.result.getVertexMemory().getProperty(vertex, PageRankVertexProgram.PAGE_RANK).get()));
    }
}
