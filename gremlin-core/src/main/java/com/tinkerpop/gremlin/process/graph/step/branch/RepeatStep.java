package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.util.TraversalHasNextPredicate;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatStep<S> extends ComputerAwareStep<S, S> implements TraversalHolder {

    private Traversal.Admin<S, S> repeatTraversal = null;
    private Predicate<Traverser<S>> untilPredicate = null;
    private Predicate<Traverser<S>> emitPredicate = null;
    public boolean untilFirst = false;
    public boolean emitFirst = false;

    private Step<?, S> endStep = null;

    public RepeatStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        requirements.add(TraverserRequirement.BULK);
        return requirements;
    }

    @SuppressWarnings("unchecked")
    public void setRepeatTraversal(final Traversal<S, S> repeatTraversal) {
        this.repeatTraversal = repeatTraversal.asAdmin(); // .clone();
        this.repeatTraversal.asAdmin().addStep(new RepeatEndStep(this.repeatTraversal));
        this.executeTraversalOperations(this.repeatTraversal, Child.SET_HOLDER, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS);
    }

    public void setUntilPredicate(final Predicate<Traverser<S>> untilPredicate) {
        if (null == this.repeatTraversal) this.untilFirst = true;
        this.untilPredicate = untilPredicate;
        if (this.untilPredicate instanceof TraversalHasNextPredicate)
            this.executeTraversalOperations(((TraversalHasNextPredicate<S,?>) this.untilPredicate).getTraversal(), Child.SET_HOLDER);
    }

    public void setEmitPredicate(final Predicate<Traverser<S>> emitPredicate) {
        if (null == this.repeatTraversal) this.emitFirst = true;
        this.emitPredicate = emitPredicate;
        if (this.emitPredicate instanceof TraversalHasNextPredicate)
            this.executeTraversalOperations(((TraversalHasNextPredicate<S,?>) this.emitPredicate).getTraversal(), Child.SET_HOLDER);
    }

    public List<Traversal<S, S>> getGlobalTraversals() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    public List<Traversal<S, ?>> getLocalTraversals() {
        final List<Traversal<S, ?>> list = new ArrayList<>();
        if (this.untilPredicate instanceof TraversalHasNextPredicate)
            list.add(((TraversalHasNextPredicate<S,?>) this.untilPredicate).getTraversal());
        if (this.emitPredicate instanceof TraversalHasNextPredicate)
            list.add(((TraversalHasNextPredicate<S,?>) this.emitPredicate).getTraversal());
        return list;
    }

    public final boolean doUntil(final Traverser<S> traverser, boolean utilFirst) {
        return utilFirst == this.untilFirst && null != this.untilPredicate && this.untilPredicate.test(traverser);
    }

    public final boolean doEmit(final Traverser<S> traverser, boolean emitFirst) {
        return emitFirst == this.emitFirst && null != this.emitPredicate && this.emitPredicate.test(traverser);
    }

    @Override
    public String toString() {
        if (this.untilFirst && this.emitFirst)
            return TraversalHelper.makeStepString(this, untilString(), emitString(), this.repeatTraversal);
        else if (this.emitFirst)
            return TraversalHelper.makeStepString(this, emitString(), this.repeatTraversal, untilString());
        else if (this.untilFirst)
            return TraversalHelper.makeStepString(this, untilString(), this.repeatTraversal, emitString());
        else
            return TraversalHelper.makeStepString(this, this.repeatTraversal, untilString(), emitString());
    }

    private final String untilString() {
        return null == this.untilPredicate ? "until(false)" : "until(" + this.untilPredicate + ")";
    }

    private final String emitString() {
        return null == this.emitPredicate ? "emit(false)" : "emit(" + this.emitPredicate + ")";
    }

    /////////////////////////

    @Override
    public RepeatStep<S> clone() throws CloneNotSupportedException {
        final RepeatStep<S> clone = (RepeatStep<S>) super.clone();
        clone.repeatTraversal = this.repeatTraversal.clone().asAdmin();
        if (this.untilPredicate instanceof TraversalHasNextPredicate)
            clone.untilPredicate = ((TraversalHasNextPredicate<S,?>) this.untilPredicate).clone();
        if (this.emitPredicate instanceof TraversalHasNextPredicate)
            clone.emitPredicate = ((TraversalHasNextPredicate<S,?>) this.emitPredicate).clone();
        clone.getGlobalTraversals().forEach(global -> clone.executeTraversalOperations(global, Child.SET_HOLDER, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS));
        clone.getLocalTraversals().forEach(local -> clone.executeTraversalOperations(local, Child.SET_HOLDER));
        return clone;
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        if (null == this.endStep) this.endStep = this.repeatTraversal.getEndStep();
        ////
        while (true) {
            if (this.endStep.hasNext()) {
                return this.endStep;
            } else {
                final Traverser.Admin<S> start = this.starts.next();
                if (doUntil(start, true)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                }
                this.repeatTraversal.asAdmin().addStart(start);
                if (doEmit(start, true)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    return IteratorUtils.of(emitSplit);
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        final Traverser.Admin<S> start = this.starts.next();
        if (doUntil(start, true)) {
            start.resetLoops();
            start.setStepId(this.getNextStep().getId());
            return IteratorUtils.of(start);
        } else {
            start.setStepId(this.repeatTraversal.asAdmin().getStartStep().getId());
            if (doEmit(start, true)) {
                final Traverser.Admin<S> emitSplit = start.split();
                emitSplit.resetLoops();
                emitSplit.setStepId(this.getNextStep().getId());
                return IteratorUtils.of(start, emitSplit);
            } else {
                return IteratorUtils.of(start);
            }
        }
    }

    /////////////////////////

    public static <A, B, C extends Traversal<A, B>> C addRepeatToTraversal(final C traversal, final Traversal<B, B> repeatTraversal) {
        final Step<?, B> step = traversal.asAdmin().getEndStep();
        if (step instanceof RepeatStep && null == ((RepeatStep) step).repeatTraversal) {
            ((RepeatStep<B>) step).setRepeatTraversal(repeatTraversal);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal);
            repeatStep.setRepeatTraversal(repeatTraversal);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    public static <A, B, C extends Traversal<A, B>> C addUntilToTraversal(final C traversal, final Predicate<Traverser<B>> untilPredicate) {
        final Step<?, B> step = traversal.asAdmin().getEndStep();
        if (step instanceof RepeatStep && null == ((RepeatStep) step).untilPredicate) {
            ((RepeatStep<B>) step).setUntilPredicate(untilPredicate);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal);
            repeatStep.setUntilPredicate(untilPredicate);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    public static <A, B, C extends Traversal<A, B>> C addEmitToTraversal(final C traversal, final Predicate<Traverser<B>> emitPredicate) {
        final Step<?, B> step = traversal.asAdmin().getEndStep();
        if (step instanceof RepeatStep && null == ((RepeatStep) step).emitPredicate) {
            ((RepeatStep<B>) step).setEmitPredicate(emitPredicate);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal);
            repeatStep.setEmitPredicate(emitPredicate);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    ///////////////////////////////////

    public class RepeatEndStep extends ComputerAwareStep<S, S> {

        public RepeatEndStep(final Traversal traversal) {
            super(traversal);
        }

        @Override
        protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
            while (true) {
                final Traverser.Admin<S> start = this.starts.next();
                start.incrLoops(this.getId());
                if (doUntil(start, false)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                } else {
                    if (!RepeatStep.this.untilFirst && !RepeatStep.this.emitFirst)
                        RepeatStep.this.repeatTraversal.addStart(start);
                    else
                        RepeatStep.this.addStart(start);
                    if (doEmit(start, false)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                }
            }
        }

        @Override
        protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
            final Traverser.Admin<S> start = this.starts.next();
            start.incrLoops(RepeatStep.this.getId());
            if (doUntil(start, false)) {
                start.resetLoops();
                start.setStepId(RepeatStep.this.getNextStep().getId());
                return IteratorUtils.of(start);
            } else {
                start.setStepId(RepeatStep.this.getId());
                if (doEmit(start, false)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    emitSplit.setStepId(RepeatStep.this.getNextStep().getId());
                    return IteratorUtils.of(start, emitSplit);
                }
                return IteratorUtils.of(start);
            }
        }
    }

}
