package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.AnnotatedListQueryBuilder;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryPipe<V> extends FlatMapPipe<Vertex, AnnotatedValue<V>> {

    public String propertyKey;
    public AnnotatedListQueryBuilder queryBuilder;
    public int low = 0;
    public int high = Integer.MAX_VALUE;
    private int counter = 0;
    private final int branchFactor;
    public boolean returnAnnotatedValues;

    public AnnotatedListQueryPipe(final Pipeline pipeline, final String propertyKey, final boolean returnAnnotatedValues, final AnnotatedListQueryBuilder queryBuilder) {
        super(pipeline);
        this.propertyKey = propertyKey;
        this.queryBuilder = queryBuilder;
        this.branchFactor = this.queryBuilder.limit;
        this.returnAnnotatedValues = returnAnnotatedValues;
        generateFunction();
    }

    public void generateFunction() {
        if (this.returnAnnotatedValues)
            this.setFunction(holder -> this.generateBuilder().build(holder.get().getValue(this.propertyKey)).annotatedValues().iterator());
        else
            this.setFunction(holder -> this.generateBuilder().build(holder.get().getValue(this.propertyKey)).values().iterator());
    }

    private AnnotatedListQueryBuilder generateBuilder() {
        final AnnotatedListQueryBuilder tempQueryBuilder = this.queryBuilder.build();
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

    protected Holder<AnnotatedValue<V>> processNextStart() {
        while (true) {
            if (this.counter > this.high) {
                throw FastNoSuchElementException.instance();
            }
            final Holder<AnnotatedValue<V>> holder = this.getNext();
            if (null != holder) {
                this.counter++;
                if (this.counter > this.low) {
                    holder.setFuture(this.getNextPipe().getAs());
                    return holder;
                }
            }
        }
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.queryBuilder);
    }
}