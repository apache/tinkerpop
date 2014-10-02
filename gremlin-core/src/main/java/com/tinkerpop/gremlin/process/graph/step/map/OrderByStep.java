package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.Comparator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderByStep<S extends Element, C extends Comparable> extends BarrierStep<S> implements Reversible {

    public Comparator comparator;
    public final String key;

    public OrderByStep(final Traversal traversal, final String key, final Comparator comparator) {
        super(traversal);
        this.key = key;
        this.comparator = comparator;
        this.setConsumer(traversers -> {
            Collections.sort(traversers, Comparator.comparing((Function<Traverser<S>, Comparable>) traverser -> traverser.get().value(key), this.comparator));
        });
    }
}
