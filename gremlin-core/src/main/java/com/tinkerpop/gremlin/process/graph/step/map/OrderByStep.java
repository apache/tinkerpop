package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Comparator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderByStep<S extends Element, C extends Comparable> extends BarrierStep<S> implements Reversible {

    private final Comparator comparator;
    private final String elementKey;

    public OrderByStep(final Traversal traversal, final String elementKey, final Comparator comparator) {
        super(traversal);
        this.elementKey = elementKey;
        this.comparator = comparator;
        this.setConsumer(traversers -> {
            traversers.sort(Comparator.comparing((Function<Traverser<S>, Comparable>) traverser -> traverser.get().value(this.elementKey), this.comparator));
        });
    }

    public String getElementKey() {
        return this.elementKey;
    }

    public Comparator getComparator() {
        return this.comparator;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.elementKey, this.comparator);
    }
}
