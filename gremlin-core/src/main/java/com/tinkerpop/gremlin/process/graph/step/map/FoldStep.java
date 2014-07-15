package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FoldStep<S> extends MapStep<S, Iterator<S>> {

    public FoldStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            final List<S> list = new ArrayList<>();
            final S s = traverser.get();
            list.add(s);
            this.getPreviousStep().forEachRemaining(t -> list.add(t.get()));
            return list.iterator();
        });
    }
}
