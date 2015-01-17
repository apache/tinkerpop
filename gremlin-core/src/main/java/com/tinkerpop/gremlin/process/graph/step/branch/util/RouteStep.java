package com.tinkerpop.gremlin.process.graph.step.branch.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RouteStep<S> extends AbstractStep<S, S> {

    private final String routeTo;   // TODO: This is just BranchWithGoToPredicate
    private final RepeatStep<S> repeatStep;   // TODO: cloning is necessary ..... ????
    private Iterator<Traverser<S>> iterator = Collections.emptyIterator();

    public RouteStep(final Traversal traversal, final String routeTo) {
        super(traversal);
        this.routeTo = routeTo;
        this.repeatStep = null;
        this.futureSetByChild = true;
    }

    public RouteStep(final Traversal traversal, final RepeatStep<S> repeatStep, final String routeTo) {
        super(traversal);
        this.routeTo = routeTo;
        this.repeatStep = repeatStep;
        this.futureSetByChild = true;
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        if (this.iterator.hasNext())
            return this.iterator.next();
        /////
        final Traverser.Admin<S> start = this.starts.next();
        if (null == this.repeatStep) {
            start.setFutureId(this.routeTo);
            return start;
        } else {
            start.incrLoops(this.repeatStep.getId());
            if (!this.repeatStep.isUntilFirst() && this.repeatStep.doUntil(start)) {
                start.resetLoops();
                start.setFutureId(this.routeTo);
                return start;
            } else {
                start.setFutureId(this.repeatStep.getId());
                if (!this.repeatStep.isEmitFirst() && this.repeatStep.doEmit(start)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    emitSplit.setFutureId(this.routeTo);
                    this.iterator = IteratorUtils.of(start);
                    return emitSplit;
                }
                return start;
            }
        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this);
    }
}
