package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.process.util.TraverserSource;
import com.tinkerpop.gremlin.process.util.SingleIterator;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends MapStep<S, S> implements TraverserSource {

    public Object start;

    public StartStep(final Traversal traversal, final Object start) {
        super(traversal);
        this.setFunction(Traverser::get);
        this.start = start;
        this.generateTraverserIterator(false);
    }

    public void clear() {
        this.starts.clear();
    }

    public void generateTraverserIterator(final boolean trackPaths) {
        if (this.start instanceof Iterator) {
            this.starts.clear();
            this.starts.add(trackPaths ? new TraverserIterator(this, (Iterator) this.start) : new TraverserIterator((Iterator) this.start));
        } else {
            this.starts.clear();
            this.starts.add(new SingleIterator(trackPaths ? new PathTraverser<>(this.getAs(), this.start) : new SimpleTraverser<>(this.start)));
        }
    }
}
