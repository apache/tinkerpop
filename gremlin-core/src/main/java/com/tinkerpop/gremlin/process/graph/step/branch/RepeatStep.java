package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.MarkerIdentityStep;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatStep<S> extends AbstractStep<S, S> implements TraversalHolder<S, S> {

    private Traversal<S, S> repeatTraversal = null;
    private Predicate<Traverser<S>> untilPredicate = null;
    private Predicate<Traverser<S>> emitPredicate = null;
    private boolean untilFirst = false;
    private boolean emitFirst = false;
    private Step<?, S> endStep = null;

    public RepeatStep(final Traversal traversal) {
        super(traversal);
    }

    public void setRepeatTraversal(final Traversal<S, S> repeatTraversal) {
        //try {
        this.repeatTraversal = repeatTraversal; // .clone()
        this.repeatTraversal.asAdmin().addStep(new MarkerIdentityStep<>(this.repeatTraversal)); // TODO: this is really bad
        final Traversal.SideEffects parentSideEffects = this.getTraversal().asAdmin().getSideEffects();
        this.repeatTraversal.asAdmin().getSideEffects().mergeSideEffects(parentSideEffects);
        this.repeatTraversal.asAdmin().setSideEffects(parentSideEffects);
        this.repeatTraversal.asAdmin().setStrategies(this.getTraversal().asAdmin().getStrategies());
        //} catch (final CloneNotSupportedException e) {
        //    throw new IllegalArgumentException(e.getMessage(), e);
        //}
    }

    public void setUntilPredicate(final Predicate<Traverser<S>> untilPredicate) {
        if (null == this.repeatTraversal) this.untilFirst = true;
        this.untilPredicate = untilPredicate;
    }

    public void setEmitPredicate(final Predicate<Traverser<S>> emitPredicate) {
        if (null == this.repeatTraversal) this.emitFirst = true;
        this.emitPredicate = emitPredicate;
    }

    public List<Traversal<S, S>> getTraversals() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    public Predicate<Traverser<S>> getUntilPredicate() {
        return this.untilPredicate;
    }

    public Predicate<Traverser<S>> getEmitPredicate() {
        return this.emitPredicate;
    }

    public boolean isUntilFirst() {
        return this.untilFirst;
    }

    public boolean isEmitFirst() {
        return this.emitFirst;
    }

    public final boolean doRepeat(final Traverser<S> traverser) {
        return null == this.untilPredicate || this.untilPredicate.test(traverser);
    }

    public final boolean doEmit(final Traverser<S> traverser) {
        return null != this.emitPredicate && this.emitPredicate.test(traverser);
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        if (null == this.endStep) this.endStep = TraversalHelper.getEnd(this.repeatTraversal);
        ////
        while (true) {
            if (this.repeatTraversal.hasNext()) {
                final Traverser.Admin<S> s = this.endStep.next().asAdmin();
                s.incrLoops();
                if (doRepeat(s)) {
                    s.resetLoops();
                    return s;
                } else {
                    this.repeatTraversal.asAdmin().addStart(s);
                    if (doEmit(s)) {
                        final Traverser.Admin<S> emitSplit = s.split();
                        emitSplit.resetLoops();
                        return emitSplit;
                    }
                }
            } else {
                final Traverser.Admin<S> s = this.starts.next();
                if (this.untilFirst && doRepeat(s)) {
                    s.resetLoops();
                    return s;
                }
                this.repeatTraversal.asAdmin().addStart(s);
                if (this.emitFirst && doEmit(s)) {
                    final Traverser.Admin<S> emitSplit = s.split();
                    emitSplit.resetLoops();
                    return emitSplit;
                }
            }
        }
    }

    @Override
    public String toString() {
        if (this.emitFirst && this.untilFirst) {
            return TraversalHelper.makeStepString(this, untilString(), emitString(), this.repeatTraversal);
        } else if (this.emitFirst && !this.untilFirst) {
            return TraversalHelper.makeStepString(this, emitString(), this.repeatTraversal, untilString());
        } else if (!this.emitFirst && this.untilFirst) {
            return TraversalHelper.makeStepString(this, untilString(), this.repeatTraversal, emitString());
        } else {
            return TraversalHelper.makeStepString(this, this.repeatTraversal, untilString(), emitString());
        }
    }

    private final String untilString() {
        return null == this.untilPredicate ? "until(true)" : "until(" + this.untilPredicate + ")";
    }

    private final String emitString() {
        return null == this.emitPredicate ? "emit(false)" : "emit(" + this.emitFirst + ")";
    }

    @Override
    public boolean requiresPaths() {
        return TraversalHelper.trackPaths(this.repeatTraversal);
    }

    //////
    public static <A, B, C extends Traversal<A, B>> C addRepeatToTraversal(final C traversal, final Traversal<B, B> repeatTraversal) {
        final Step<?, B> step = TraversalHelper.getEnd(traversal);
        if (step instanceof RepeatStep && ((RepeatStep) step).getTraversals().isEmpty()) {
            ((RepeatStep<B>) step).setRepeatTraversal(repeatTraversal);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal);
            repeatStep.setRepeatTraversal(repeatTraversal);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    public static <A, B, C extends Traversal<A, B>> C addUntilToTraversal(final C traversal, final Predicate<Traverser<B>> untilPredicate) {
        final Step<?, B> step = TraversalHelper.getEnd(traversal);
        if (step instanceof RepeatStep && null == ((RepeatStep) step).getUntilPredicate()) {
            ((RepeatStep<B>) step).setUntilPredicate(untilPredicate);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal);
            repeatStep.setUntilPredicate(untilPredicate);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    public static <A, B, C extends Traversal<A, B>> C addEmitToTraversal(final C traversal, final Predicate<Traverser<B>> emitPredicate) {
        final Step<?, B> step = TraversalHelper.getEnd(traversal);
        if (step instanceof RepeatStep && null == ((RepeatStep) step).getEmitPredicate()) {
            ((RepeatStep<B>) step).setEmitPredicate(emitPredicate);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal);
            repeatStep.setEmitPredicate(emitPredicate);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }
    //////

    public static class LoopPredicate<S> implements Predicate<Traverser<S>> {
        private final int maxLoops;

        public LoopPredicate(final int maxLoops) {
            this.maxLoops = maxLoops;
        }

        @Override
        public boolean test(final Traverser<S> traverser) {
            return traverser.loops() >= this.maxLoops;
        }

        @Override
        public String toString() {
            return "loops(" + this.maxLoops + ")";
        }
    }
}
