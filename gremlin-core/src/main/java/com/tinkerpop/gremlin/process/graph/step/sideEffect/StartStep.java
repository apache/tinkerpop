package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserIterator;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends SideEffectStep<S> implements TraverserSource, Reversible {

    protected Object start;

    public StartStep(final Traversal traversal, final Object start) {
        super(traversal);
        this.start = start;
    }

    public StartStep(final Traversal traversal) {
        this(traversal, null);
    }

    @Override
    public void clear() {
        this.starts.clear();
    }

    public String toString() {
        return null == this.start ? TraversalHelper.makeStepString(this) : TraversalHelper.makeStepString(this, this.start);
    }

    @Override
    public void generateTraverserIterator(final boolean trackPaths) {
        if (null != this.start) {
            this.starts.clear();
            if (this.start instanceof Iterator)
                this.starts.add(new TraverserIterator(this, trackPaths, (Iterator) this.start));
            else
                this.starts.add((Traverser.Admin) (trackPaths ? new PathTraverser<>(this.getLabel(), this.start, this.traversal.sideEffects()) : new SimpleTraverser<>(this.start,this.traversal.sideEffects())));
        }
    }
}
