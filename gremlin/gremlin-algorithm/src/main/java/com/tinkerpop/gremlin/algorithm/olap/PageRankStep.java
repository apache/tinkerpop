package com.tinkerpop.gremlin.algorithm.olap;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.ComputeResult;
import com.tinkerpop.gremlin.process.steps.AbstractStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankStep extends AbstractStep<Vertex, Vertex> {

    private final Graph graph;
    private boolean firstCall = true;
    private Iterator<Vertex> itty;
    private ComputeResult result;

    public PageRankStep(final Traversal traversal, final Graph graph) {
        super(traversal);
        this.graph = graph;
    }

    public Holder<Vertex> processNextStart() {
        try {
            if (this.firstCall) {
                //this.getPreviousStep().forEachRemaining(null);
                this.result = this.graph.compute().program(PageRankVertexProgram.create().build()).submit().get();
                this.firstCall = false;
                this.itty = graph.V();
            }
            return new SimpleHolder<>(this.itty.next());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }
}
