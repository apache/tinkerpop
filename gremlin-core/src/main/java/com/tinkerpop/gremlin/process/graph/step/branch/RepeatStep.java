package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatStep<S> extends AbstractStep<S, S> implements EngineDependent {

    private Traversal<S, S> repeatTraversal;
    private Predicate<Traverser<S>> until = null;
    private Predicate<Traverser<S>> emit = null;
    boolean untilFirst = false;
    boolean emitFirst = false;
    boolean onGraphComputer = false;

    public RepeatStep(final Traversal traversal) {
        super(traversal);
    }

    public void setRepeatTraversal(final Traversal<S, S> repeatTraversal) {
        this.repeatTraversal = repeatTraversal;
    }

    public void setUntilPredicate(final Predicate<Traverser<S>> untilPredicate) {
        if (null == this.repeatTraversal) this.untilFirst = true;
        this.until = untilPredicate;
    }

    public void setEmitPredicate(final Predicate<Traverser<S>> emitPredicate) {
        if (null == this.emit) this.emitFirst = true;
        this.emit = emitPredicate;
    }

    public Traversal<S, S> getRepeatTraversal() {
        return this.repeatTraversal;
    }

    public Predicate<Traverser<S>> getUntilPredicate() {
        return this.until;
    }

    public Predicate<Traverser<S>> getEmitPredicate() {
        return this.emit;
    }

    public boolean isUntilFirst() {
        return this.untilFirst;
    }

    protected Traverser<S> processNextStart() throws NoSuchElementException {
        return this.onGraphComputer ? computerAlgorithm() : standardAlgorithm();
    }

    public JumpStep<S> createJumpStep(final String headLabel) {
        final JumpStep.Builder<S> builder = JumpStep.<S>build(this.getTraversal()).jumpLabel(headLabel);
        if (null != this.until) {
            if (this.until instanceof LoopPredicate)
                builder.jumpLoops(((LoopPredicate) this.until).maxLoops, Compare.lt);
            else
                builder.jumpPredicate(this.until.negate());
        } else
            builder.jumpChoice(true);
        if (null != this.emit)
            builder.emitPredicate(this.emit);
        else
            builder.emitChoice(false);
        return builder.create();
    }

    public UntilStep<S> createUntilStep(final String tailLabel) {
        return new UntilStep<>(this.getTraversal(), tailLabel, this.until, this.emit);
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.onGraphComputer = traversalEngine.equals(TraversalEngine.COMPUTER);
    }

    ///

    public static class LoopPredicate<S> implements Predicate<Traverser<S>> {
        private final int maxLoops;

        public LoopPredicate(final int maxLoops) {
            this.maxLoops = maxLoops;
        }

        @Override
        public boolean test(final Traverser<S> traverser) {
            return traverser.loops() >= this.maxLoops;
        }
    }

    private Traverser<S> computerAlgorithm() throws NoSuchElementException {
        throw new IllegalStateException("RepeatStep should be compiled down to JumpSteps when used with GraphComputer");
    }

    private Traverser<S> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.repeatTraversal.hasNext()) {
                final Traverser.Admin<S> s = TraversalHelper.getEnd(this.repeatTraversal).next().asAdmin();
                s.incrLoops();
                if (this.until.test(s)) {
                    s.resetLoops();
                    return s;
                } else {
                    this.repeatTraversal.asAdmin().addStart(s);
                    if (null != this.emit && this.emit.test(s)) {
                        final Traverser.Admin<S> emitSplit = s.split();
                        emitSplit.resetLoops();
                        return emitSplit;
                    }
                }
            } else {
                final Traverser.Admin<S> s = this.starts.next();
                s.resetLoops();
                if (this.untilFirst && this.until.test(s)) {
                    s.resetLoops();
                    return s;
                }
                this.repeatTraversal.asAdmin().addStart(s);
                if (this.emitFirst && null != this.emit && this.emit.test(s)) {
                    final Traverser.Admin<S> emitSplit = s.split();
                    emitSplit.resetLoops();
                    return emitSplit;
                }
            }

        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.repeatTraversal);
    }
}
