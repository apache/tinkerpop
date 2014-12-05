package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
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
    private Traversal constraint;


    public WhereStep(final Traversal traversal, final String firstKey, final String secondKey, final BiPredicate<E, E> biPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = secondKey;
        this.biPredicate = biPredicate;
        this.constraint = null;
        WhereStep.generatePredicate(this);

    }

    public WhereStep(final Traversal traversal, final Traversal constraint) {
        super(traversal);
        this.firstKey = null;
        this.secondKey = null;
        this.biPredicate = null;
        this.constraint = constraint;
        WhereStep.generatePredicate(this);
    }

    public boolean hasBiPredicate() {
        return null != this.biPredicate;
    }

    public Traversal getConstraint() {
        return this.constraint;
    }

    @Override
    public WhereStep<E> clone() throws CloneNotSupportedException {
        final WhereStep<E> clone = (WhereStep<E>) super.clone();
        if (null != this.constraint) clone.constraint = this.constraint.clone();
        WhereStep.generatePredicate(clone);
        return clone;
    }

    /////////////////////////

    private static final <E> void generatePredicate(final WhereStep<E> whereStep) {
        if (null == whereStep.constraint) {
            whereStep.setPredicate(traverser -> {
                final Map<String, E> map = traverser.get();
                if (!map.containsKey(whereStep.firstKey))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + whereStep.firstKey);
                if (!map.containsKey(whereStep.secondKey))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + whereStep.secondKey);
                return whereStep.biPredicate.test(map.get(whereStep.firstKey), map.get(whereStep.secondKey));
            });
        } else {
            final Step startStep = TraversalHelper.getStart(whereStep.constraint);
            final Step endStep = TraversalHelper.getEnd(whereStep.constraint);
            whereStep.setPredicate(traverser -> {
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

                // TODO: Can we add LocalStep here?
                startStep.addStart(whereStep.getTraversal().asAdmin().getTraverserGenerator().generate(startObject, startStep, traverser.bulk()));
                if (null == endObject) {
                    if (whereStep.constraint.hasNext()) {
                        whereStep.constraint.asAdmin().reset();
                        return true;
                    } else {
                        return false;
                    }

                } else {
                    while (whereStep.constraint.hasNext()) {
                        if (whereStep.constraint.next().equals(endObject)) {
                            whereStep.constraint.asAdmin().reset();
                            return true;
                        }
                    }
                    return false;
                }
            });
        }
    }


}
