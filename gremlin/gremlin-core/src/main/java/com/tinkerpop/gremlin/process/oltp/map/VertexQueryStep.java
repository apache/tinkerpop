package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryStep<E extends Element> extends FlatMapStep<Vertex, E> {

    public VertexQueryBuilder queryBuilder;
    public Class<E> returnClass;
    public int low = 0;
    public int high = Integer.MAX_VALUE;
    private int counter = 0;
    private final int branchFactor;

    public VertexQueryStep(final Traversal traversal, final VertexQueryBuilder queryBuilder, final Class<E> returnClass) {
        super(traversal);
        this.queryBuilder = queryBuilder;
        this.branchFactor = this.queryBuilder.limit;
        this.returnClass = returnClass;
        generateFunction();
    }

    public void generateFunction() {
        if (this.returnClass.equals(Vertex.class))
            this.setFunction(holder -> (Iterator<E>) this.generateBuilder().build(holder.get()).vertices().iterator());
        else
            this.setFunction(holder -> (Iterator<E>) this.generateBuilder().build(holder.get()).edges().iterator());
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

    protected Holder<E> processNextStart() {
        while (true) {
            if (this.counter > this.high) {
                throw FastNoSuchElementException.instance();
            }
            final Holder<E> holder = this.getNext();
            if (null != holder) {
                this.counter++;
                if (this.counter > this.low) {
                    holder.setFuture(this.getNextStep().getAs());
                    return holder;
                }
            }
        }
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase(), this.queryBuilder);
    }

}
