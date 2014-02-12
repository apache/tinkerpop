package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.AnnotatedListQueryBuilder;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryStep<V> extends FlatMapStep<Vertex, AnnotatedValue<V>> {

    public String propertyKey;
    public AnnotatedListQueryBuilder queryBuilder;
    public int low = 0;
    public int high = Integer.MAX_VALUE;
    private int counter = 0;
    private final int branchFactor;

    public AnnotatedListQueryStep(final Traversal traversal, final String propertyKey, final AnnotatedListQueryBuilder queryBuilder) {
        super(traversal);
        this.propertyKey = propertyKey;
        this.queryBuilder = queryBuilder;
        this.branchFactor = this.queryBuilder.limit;
        generateFunction(true);
    }

    public void generateFunction(final boolean returnAnnotatedValues) {
        if (returnAnnotatedValues)
            this.setFunction(holder -> {
                final Property<AnnotatedList> property = holder.get().getProperty(this.propertyKey);
                return (property.isPresent()) ?
                        this.generateBuilder().build(holder.get().getValue(this.propertyKey)).annotatedValues().iterator() :
                        Collections.emptyIterator();

            });
        else
            this.setFunction(holder -> {
                final Property<AnnotatedList> property = holder.get().getProperty(this.propertyKey);
                return (property.isPresent()) ?
                        this.generateBuilder().build(holder.get().getValue(this.propertyKey)).values().iterator() :
                        Collections.emptyIterator();

            });
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
                    holder.setFuture(this.getNextStep().getAs());
                    return holder;
                }
            }
        }
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.queryBuilder);
    }
}