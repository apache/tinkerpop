package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.HolderSource;
import com.tinkerpop.gremlin.process.steps.util.SingleIterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends MapStep<S, S> implements HolderSource {

    public S start;

    public StartStep(final Traversal traversal, final S start) {
        super(traversal);
        this.start = start;
        this.setFunction(Holder::get);
    }

    public void generateHolderIterator(final boolean trackPaths) {
        final Holder<S> holder = trackPaths ? new PathHolder<>(this.getAs(), this.start) : new SimpleHolder<>(this.start);
        holder.setFuture(this.getNextStep().getAs());
        this.starts.clear();
        this.starts.add(new SingleIterator(holder));
    }
}
