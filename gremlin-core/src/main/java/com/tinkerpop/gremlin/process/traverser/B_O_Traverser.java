package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_Traverser<T> extends O_Traverser<T> {

    protected long bulk = 1l;
    protected String future = HALT;

    protected B_O_Traverser() {
    }

    public B_O_Traverser(final T t, final long initialBulk) {
        super(t);
        this.bulk = initialBulk;
    }

    @Override
    public void setBulk(final long count) {
        this.bulk = count;
    }

    @Override
    public long bulk() {
        return this.bulk;
    }

    @Override
    public void merge(final Traverser.Admin<?> other) {
        this.bulk = this.bulk + other.bulk();
    }

    @Override
    public String getFutureId() {
        return this.future;
    }

    @Override
    public void setFutureId(final String stepId) {
        this.future = stepId;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof B_O_Traverser &&
                ((B_O_Traverser) object).get().equals(this.t) &&
                ((B_O_Traverser) object).getFutureId().equals(this.future);
    }
}
