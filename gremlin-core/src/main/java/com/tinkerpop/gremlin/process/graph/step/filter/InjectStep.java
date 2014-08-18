package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraverserIterator;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class InjectStep<S> extends FilterStep<S> implements TraverserSource {

    private final List<S> injections;

    @SafeVarargs
    public InjectStep(final Traversal traversal, final S... injections) {
        super(traversal);
        this.setPredicate(t -> true);
        this.injections = Arrays.asList(injections);

    }

    public void generateTraverserIterator(final boolean trackPaths) {
        if (trackPaths)
            this.addStarts(new TraverserIterator<>(this, this.injections.iterator()));
        else
            this.addStarts(new TraverserIterator<>(this.injections.iterator()));
    }

    public void clear() {
        this.starts.clear();
    }
}
