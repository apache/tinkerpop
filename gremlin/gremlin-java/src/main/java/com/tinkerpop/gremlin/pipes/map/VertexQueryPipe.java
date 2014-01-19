package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.FlatMapPipe;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryPipe<E extends Element> extends FlatMapPipe<Vertex, E> {

    public VertexQueryBuilder queryBuilder;
    public Class<E> returnClass;
    public int low = 0;
    public int high = Integer.MAX_VALUE;
    private int counter = 0;
    private final int branchFactor;

    public VertexQueryPipe(final Pipeline pipeline, final VertexQueryBuilder queryBuilder, final Class<E> returnClass) {
        super(pipeline);
        this.queryBuilder = queryBuilder;
        this.branchFactor = this.queryBuilder.limit;
        this.returnClass = returnClass;
        generateFunction();
    }

    public void generateFunction() {
        if (this.returnClass.equals(Vertex.class))
            this.setFunction(v -> (Iterator<E>) this.generateBuilder().build(v.get()).vertices().iterator());
        else
            this.setFunction(v -> (Iterator<E>) this.generateBuilder().build(v.get()).edges().iterator());
    }

    private VertexQueryBuilder generateBuilder() {
        final VertexQueryBuilder tempQueryBuilder = this.queryBuilder.build();
        if (this.branchFactor == Integer.MAX_VALUE) {
            if (this.high != Integer.MAX_VALUE) {
                int temp = (1 + this.high) - this.counter;
                if (temp > 0) tempQueryBuilder.limit(temp);
            }
        } else {
            if (this.high == Integer.MAX_VALUE) {
                tempQueryBuilder.limit(this.branchFactor);
            } else {
                int temp = (1 + this.high) - this.counter;
                tempQueryBuilder.limit(temp < this.branchFactor ? temp : this.branchFactor);
            }
        }
        return this.queryBuilder;
    }

    public Holder<E> processNextStart() {
        while (true) {
            if (this.counter > this.high) {
                throw FastNoSuchElementException.instance();
            }
            final Holder<E> holder = this.getNext();
            if (null != holder) {
                this.counter++;
                if (this.counter > this.low) {
                    holder.setFuture(GremlinHelper.getNextPipeLabel(this.pipeline, this));
                    return holder;
                }
            }
        }
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase(), this.queryBuilder);
    }

}
