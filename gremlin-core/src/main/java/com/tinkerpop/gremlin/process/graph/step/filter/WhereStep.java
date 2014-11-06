package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Map;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStep<E> extends FilterStep<Map<String, E>> {

    private final String firstKey;
    private final String secondKey;
    private final BiPredicate biPredicate;
    private final Traversal constraint;


    public WhereStep(final Traversal traversal, final String firstKey, final String secondKey, final BiPredicate<E, E> biPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = secondKey;
        this.biPredicate = biPredicate;
        this.constraint = null;

        this.setPredicate(traverser -> {
            final Map<String, E> map = traverser.get();
            if (!map.containsKey(this.firstKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.firstKey);
            if (!map.containsKey(this.secondKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.secondKey);
            return biPredicate.test(map.get(this.firstKey), map.get(this.secondKey));
        });
    }

    public WhereStep(final Traversal traversal, final Traversal constraint) {
        super(traversal);
        this.firstKey = null;
        this.secondKey = null;
        this.biPredicate = null;
        this.constraint = constraint;

        final Step startStep = TraversalHelper.getStart(constraint);
        final Step endStep = TraversalHelper.getEnd(constraint);

        this.setPredicate(traverser -> {
            final Map<String, E> map = traverser.get();
            if (!map.containsKey(startStep.getLabel()))
                throw new IllegalArgumentException("The provided key is not in the current map: " + startStep.getLabel());
            final Object startObject = map.get(startStep.getLabel());
            final Object endObject;
            if (TraversalHelper.isLabeled(endStep)) {
                if (!map.containsKey(endStep.getLabel()))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + endStep.getLabel());
                endObject = map.get(endStep.getLabel());
            } else
                endObject = null;

            startStep.addStart(TraversalStrategies.GlobalCache.getStrategies(constraint.getClass()).getTraverserGenerator(constraint, TraversalEngine.STANDARD).generate(startObject, startStep));
            if (null == endObject) {
                if (constraint.hasNext()) {
                    constraint.reset();
                    return true;
                } else {
                    return false;
                }

            } else {
                while (constraint.hasNext()) {
                    if (constraint.next().equals(endObject)) {
                        constraint.reset();
                        return true;
                    }
                }
                return false;
            }
        });
    }

    public boolean hasBiPredicate() {
        return null != this.biPredicate;
    }

    public Traversal getConstraint() {
        return this.constraint;
    }


}
