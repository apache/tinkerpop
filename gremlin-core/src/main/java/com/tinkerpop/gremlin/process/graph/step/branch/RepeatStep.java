package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatStep<S> extends ComputerAwareStep<S, S> implements TraversalHolder<S, S> {

    private static final Child[] CHILD_OPERATIONs = new Child[]{Child.SET_HOLDER, Child.SET_STRATEGIES, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS};

    private Traversal<S, S> repeatTraversal = null;
    private Predicate<Traverser<S>> untilPredicate = null;
    private Predicate<Traverser<S>> emitPredicate = null;
    private boolean untilFirst = false;
    private boolean emitFirst = false;
    private Step<?, S> endStep = null;

    public RepeatStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getTraversalRequirements();
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        requirements.add(TraverserRequirement.BULK);
        return requirements;
    }

    @Override
    public TraversalStrategies getChildStrategies() {
        return TraversalHolder.super.getChildStrategies().removeStrategies(SideEffectCapStrategy.class); // no auto cap();
    }

    @SuppressWarnings("unchecked")
    public void setRepeatTraversal(final Traversal<S, S> repeatTraversal) {
        this.repeatTraversal = repeatTraversal; // .clone();
        this.executeTraversalOperations(CHILD_OPERATIONs);
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

    public final boolean doUntil(final Traverser<S> traverser) {
        return null != this.untilPredicate && this.untilPredicate.test(traverser);
    }

    public final boolean doEmit(final Traverser<S> traverser) {
        return null != this.emitPredicate && this.emitPredicate.test(traverser);
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
        return null == this.untilPredicate ? "until(false)" : "until(" + this.untilPredicate + ")";
    }

    private final String emitString() {
        return null == this.emitPredicate ? "emit(false)" : "emit(" + this.emitFirst + ")";
    }

    /////////////////////////

    @Override
    public RepeatStep<S> clone() throws CloneNotSupportedException {
        final RepeatStep<S> clone = (RepeatStep<S>) super.clone();
        clone.repeatTraversal = this.repeatTraversal.clone();
        clone.executeTraversalOperations(CHILD_OPERATIONs);

        if (this.untilPredicate instanceof TraversalPredicate)
            clone.untilPredicate = ((TraversalPredicate<S>) this.untilPredicate).clone();

        if (this.emitPredicate instanceof TraversalPredicate)
            clone.emitPredicate = ((TraversalPredicate<S>) this.emitPredicate).clone();

        return clone;
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        if (null == this.endStep) this.endStep = TraversalHelper.getEnd(this.repeatTraversal);
        ////
        while (true) {
            if (this.repeatTraversal.hasNext()) {
                final Traverser.Admin<S> start = this.endStep.next().asAdmin();
                start.incrLoops(this.getId());
                if (doUntil(start)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                } else {
                    this.repeatTraversal.asAdmin().addStart(start);
                    if (doEmit(start)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                }
            } else {
                final Traverser.Admin<S> s = this.starts.next();
                if (this.untilFirst && doUntil(s)) {
                    s.resetLoops();
                    return IteratorUtils.of(s);
                }
                this.repeatTraversal.asAdmin().addStart(s);
                if (this.emitFirst && doEmit(s)) {
                    final Traverser.Admin<S> emitSplit = s.split();
                    emitSplit.resetLoops();
                    return IteratorUtils.of(emitSplit);
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        final Traverser.Admin<S> start = this.starts.next();
        if (this.untilFirst && doUntil(start)) {
            start.resetLoops();
            start.setFutureId(this.getNextStep().getId());
            return IteratorUtils.of(start);
        } else {
            start.setFutureId(TraversalHelper.getStart(this.repeatTraversal).getId());
            if (this.emitFirst && doEmit(start)) {
                final Traverser.Admin<S> emitSplit = start.split();
                emitSplit.resetLoops();
                emitSplit.setFutureId(this.getNextStep().getId());
                return IteratorUtils.of(start, emitSplit);
            } else {
                return IteratorUtils.of(start);
            }
        }
    }

    /////////////////////////

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

    // TODO: linearize this for OLAP!!
    public static class TraversalPredicate<S> implements Predicate<Traverser<S>>, Cloneable {

        private Traversal<S, ?> traversal;

        public TraversalPredicate(final Traversal<S, ?> traversal) {
            this.traversal = traversal;
        }

        @Override
        public boolean test(final Traverser<S> traverser) {
            this.traversal.asAdmin().reset();
            this.traversal.asAdmin().addStart(traverser.asAdmin().split());
            return this.traversal.hasNext();
        }

        @Override
        public String toString() {
            return this.traversal.toString();
        }

        @Override
        public TraversalPredicate<S> clone() throws CloneNotSupportedException {
            final TraversalPredicate<S> clone = (TraversalPredicate<S>) super.clone();
            clone.traversal = this.traversal.clone();
            return clone;
        }
    }
}
