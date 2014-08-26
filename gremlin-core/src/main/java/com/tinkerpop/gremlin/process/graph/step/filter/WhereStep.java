package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.util.function.SBiPredicate;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WhereStep<E> extends FilterStep<Map<String, E>> {

    public final String firstKey;
    public final String secondKey;
    public final SBiPredicate biPredicate;
    public final Traversal constraint;


    public WhereStep(final Traversal traversal, final String firstKey, final String secondKey, final SBiPredicate biPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = secondKey;
        this.biPredicate = biPredicate;
        this.constraint = null;

        this.setPredicate(traverser -> {
            final Map<String, E> map = traverser.get();
            return biPredicate.test(map.get(firstKey), map.get(secondKey));
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
            final Object startObject = map.get(startStep.getAs());
            final Object endObject = TraversalHelper.isLabeled(endStep) ? map.get(endStep.getAs()) : null;
            startStep.addStarts(new TraverserIterator<>(startStep, TraversalHelper.trackPaths(constraint), Arrays.asList(startObject).iterator()));
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


}
