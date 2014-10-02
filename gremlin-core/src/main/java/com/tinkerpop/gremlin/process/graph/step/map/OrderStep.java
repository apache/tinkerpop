package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;

import java.util.Collections;
import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderStep<S> extends BarrierStep<S> implements Reversible {

    public Comparator<Traverser<S>> comparator;

    public OrderStep(final Traversal traversal, final Comparator<Traverser<S>> comparator) {
        super(traversal);
        this.comparator = comparator;
        this.setConsumer(traversers -> Collections.sort(traversers, this.comparator));
    }
}
