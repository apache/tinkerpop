package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.HolderIterator;
import com.tinkerpop.gremlin.process.util.HolderSource;
import com.tinkerpop.gremlin.process.util.SingleIterator;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends MapStep<S, S> implements HolderSource {

    public Object start;

    public StartStep(final Traversal traversal, final Object start) {
        super(traversal, Holder::get);
        this.start = start;
        this.generateHolderIterator(false);
    }

    public void clear() {
        this.starts.clear();
    }

    public void generateHolderIterator(final boolean trackPaths) {
        if (this.start instanceof Iterator) {
            this.starts.clear();
            this.starts.add(trackPaths ? new HolderIterator(this, (Iterator) this.start) : new HolderIterator((Iterator) this.start));
        } else {
            this.starts.clear();
            this.starts.add(new SingleIterator(trackPaths ? new PathHolder<>(this.getAs(), this.start) : new SimpleHolder<>(this.start)));
        }
    }
}
