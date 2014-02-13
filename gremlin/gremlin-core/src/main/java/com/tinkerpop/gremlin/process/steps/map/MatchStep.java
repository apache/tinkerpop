package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.AbstractStep;
import com.tinkerpop.gremlin.process.steps.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStep<S, E> extends AbstractStep<S, E> {

    private final Map<String, List<Traversal>> predicateTraversals = new HashMap<>();
    private final Map<String, List<Traversal>> internalTraversals = new HashMap<>();
    private Traversal endTraversal = null;
    private String endTraversalStartAs;
    private final String inAs;
    private final String outAs;

    public MatchStep(final Traversal traversal, final String inAs, final String outAs, final Traversal... traversals) {
        super(traversal);
        this.inAs = inAs;
        this.outAs = outAs;
        for (final Traversal p1 : traversals) {
            final String start = TraversalHelper.getStart(p1).getAs();
            final String end = TraversalHelper.getEnd(p1).getAs();
            if (!TraversalHelper.isLabeled(start)) {
                throw new IllegalArgumentException("All match traversals must have their start pipe labeled");
            }
            if (!TraversalHelper.isLabeled(end)) {
                final List<Traversal> list = this.predicateTraversals.getOrDefault(start, new ArrayList<>());
                this.predicateTraversals.put(start, list);
                list.add(p1);
            } else {
                if (end.equals(this.outAs)) {
                    if (null != this.endTraversal)
                        throw new IllegalArgumentException("There can only be one outAs labeled end traversal");
                    this.endTraversal = p1;
                    this.endTraversalStartAs = TraversalHelper.getStart(p1).getAs();
                } else {
                    final List<Traversal> list = this.internalTraversals.getOrDefault(start, new ArrayList<>());
                    this.internalTraversals.put(start, list);
                    list.add(p1);
                }
            }
        }
        if (null == this.endTraversal)
            throw new IllegalStateException("One of the match traversals must be an end traversal");
    }

    protected Holder<E> processNextStart() {
        while (true) {
            if (this.endTraversal.hasNext()) {
                final Holder<E> holder = (Holder<E>) TraversalHelper.getEnd(this.endTraversal).next();
                if (doPredicates(this.outAs, holder)) {
                    return holder;
                }
            } else {
                final Holder temp = this.starts.next();
                temp.getPath().renameLastStep(this.inAs); // TODO: is this cool?
                doMatch(this.inAs, temp);
            }
        }
    }

    private void doMatch(final String as, final Holder holder) {
        if (!doPredicates(as, holder))
            return;

        if (as.equals(this.endTraversalStartAs)) {
            this.endTraversal.addStarts(new SingleIterator<>(holder));
            return;
        }

        for (final Traversal traversal : this.internalTraversals.get(as)) {
            traversal.addStarts(new SingleIterator<>(holder));
            final Step<?, ?> endStep = TraversalHelper.getEnd(traversal);
            while (endStep.hasNext()) {
                final Holder temp = endStep.next();
                doMatch(endStep.getAs(), temp);
            }
        }
    }

    private boolean doPredicates(final String as, final Holder holder) {
        if (this.predicateTraversals.containsKey(as)) {
            for (final Traversal traversal : this.predicateTraversals.get(as)) {
                traversal.addStarts(new SingleIterator<>(holder));
                if (!TraversalHelper.hasNextIteration(traversal))
                    return false;
            }
        }
        return true;
    }

}
