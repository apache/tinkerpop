package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Compare;
import org.javatuples.Pair;

import java.util.Queue;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JumpStep<S> extends AbstractStep<S, S> implements EngineDependent {

    private String jumpLabel;
    private Step jumpToStep;
    /////////////////////
    private Predicate<Traverser<S>> jumpPredicate;
    private Pair<Short, Compare> jumpLoops;
    private Boolean jumpChoice;
    /////////////////////
    private Predicate<Traverser<S>> emitPredicate;
    private Boolean emitChoice;
    /////////////////////
    private Boolean jumpBack;
    private boolean onGraphComputer = false;
    private Queue<Traverser.Admin<S>> queue;
    public boolean doWhile = true;

    public JumpStep(final Traversal traversal) {
        super(traversal);
        this.futureSetByChild = true;
    }

    public void onEngine(final Engine engine) {
        if (engine.equals(Engine.COMPUTER)) {
            this.onGraphComputer = true;
            this.queue = new TraverserSet<>();
        } else {
            this.onGraphComputer = false;
            this.queue = null;
        }
    }

    @Override
    protected Traverser<S> processNextStart() {
        return this.onGraphComputer ? computerAlgorithm() : standardAlgorithm();
    }

    private Traverser<S> standardAlgorithm() {
        if (null == this.jumpToStep) {
            this.jumpToStep = TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep();
            this.jumpBack = TraversalHelper.relativeLabelDirection(this, this.jumpLabel) == -1;
            // TODO: getNextStep() may be dependent on whether its a jump back or a jump forward
        }
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            if (PROFILING_ENABLED) TraversalMetrics.start(this);
            if (this.jumpBack) traverser.incrLoops();
            if (doJump(traverser)) {
                traverser.setFuture(this.jumpLabel);
                this.jumpToStep.addStart(traverser);
                if (doEmit(traverser)) {
                    final Traverser.Admin<S> emitTraverser = traverser.makeSibling();
                    if (this.jumpBack) emitTraverser.resetLoops();
                    emitTraverser.setFuture(this.getNextStep().getLabel());
                    if (PROFILING_ENABLED) TraversalMetrics.finish(this, traverser);
                    return emitTraverser;
                }
            } else {
                if (this.jumpBack) traverser.resetLoops();
                traverser.setFuture(this.getNextStep().getLabel());
                if (PROFILING_ENABLED) TraversalMetrics.finish(this, traverser);
                return traverser;
            }

            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }

    private Traverser<S> computerAlgorithm() {
        final String loopFuture = TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep().getLabel();
        if (null == this.jumpBack)
            this.jumpBack = TraversalHelper.relativeLabelDirection(this, this.jumpLabel) == -1;
        while (true) {
            if (!this.queue.isEmpty()) {
                Traverser.Admin<S> ret = this.queue.remove();
                if (PROFILING_ENABLED) TraversalMetrics.finish(this, ret);
                return ret;
            } else {
                final Traverser.Admin<S> traverser = this.starts.next();
                if (PROFILING_ENABLED) TraversalMetrics.start(this);

                if (this.jumpBack) traverser.incrLoops();
                if (doJump(traverser)) {
                    traverser.setFuture(loopFuture);
                    this.queue.add(traverser);
                    if (doEmit(traverser)) {
                        final Traverser.Admin<S> emitTraverser = traverser.makeSibling();
                        if (this.jumpBack) emitTraverser.resetLoops();
                        emitTraverser.setFuture(this.nextStep.getLabel());
                        this.queue.add(emitTraverser);
                    }
                } else {
                    if (this.jumpBack) traverser.resetLoops();
                    traverser.setFuture(this.nextStep.getLabel());
                    this.queue.add(traverser);
                }

                if (PROFILING_ENABLED) TraversalMetrics.stop(this);
            }
        }
    }

    public boolean unRollable() {
        return !this.onGraphComputer && this.jumpLoops != null && null == this.emitPredicate && !this.emitChoice;
    }

    public boolean isDoWhile() {
        return this.doWhile;
    }

    private Boolean doJump(final Traverser<S> traverser) {
        if (null != this.jumpChoice)
            return this.jumpChoice;
        else if (null != this.jumpLoops)
            return this.jumpLoops.getValue1().test(traverser.loops(), this.jumpLoops.getValue0());
        else
            return this.jumpPredicate.test(traverser);
    }

    private Boolean doEmit(final Traverser<S> traverser) {
        return null != this.emitChoice ? this.emitChoice : this.emitPredicate.test(traverser);
    }

    public String getJumpLabel() {
        return this.jumpLabel;
    }

    public Pair<Short, Compare> getJumpLoops() {
        return this.jumpLoops;
    }

    public String toString() {
        return null != this.jumpLoops ?
                TraversalHelper.makeStepString(this, this.jumpLabel, this.jumpLoops.getValue1().asString() + this.jumpLoops.getValue0()) :
                TraversalHelper.makeStepString(this, this.jumpLabel);
    }

    public static <S> Builder<S> build(final Traversal traversal) {
        return new Builder<>(traversal);
    }

    ////////////////

    public static class Builder<S> {

        private String jumpLabel = null;

        private Predicate<Traverser<S>> jumpPredicate = null;
        private Pair<Short, Compare> jumpLoops = null;
        private Boolean jumpChoice = null;

        private Predicate<Traverser<S>> emitPredicate = null;
        private Boolean emitChoice = null;

        private Traversal traversal = null;

        public Builder(final Traversal traversal) {
            this.traversal = traversal;
        }

        public Builder<S> emitPredicate(final Predicate<Traverser<S>> emitPredicate) {
            this.emitPredicate = emitPredicate;
            this.emitChoice = null;
            return this;
        }

        public Builder<S> emitChoice(final boolean emitChoice) {
            this.emitChoice = emitChoice;
            this.emitPredicate = null;
            return this;
        }

        //

        public Builder<S> jumpPredicate(final Predicate<Traverser<S>> jumpPredicate) {
            this.jumpPredicate = jumpPredicate;
            this.jumpLoops = null;
            this.jumpChoice = null;
            return this;
        }

        public Builder<S> jumpChoice(final boolean jumpChoice) {
            this.jumpChoice = jumpChoice;
            this.jumpLoops = null;
            this.jumpPredicate = null;
            return this;
        }

        public Builder<S> jumpLoops(final int jumpLoops, final Compare loopCompare) {
            this.jumpLoops = Pair.with((short) jumpLoops, loopCompare);
            this.jumpPredicate = null;
            this.jumpChoice = null;
            return this;
        }

        //

        public Builder<S> jumpLabel(final String jumpLabel) {
            this.jumpLabel = jumpLabel;
            return this;
        }

        public JumpStep<S> create() {
            final JumpStep<S> jumpStep = new JumpStep<>(this.traversal);
            jumpStep.jumpLabel = this.jumpLabel;
            jumpStep.jumpLoops = this.jumpLoops;
            jumpStep.jumpChoice = this.jumpChoice;
            jumpStep.emitChoice = this.emitChoice;
            jumpStep.jumpPredicate = this.jumpPredicate;
            jumpStep.emitPredicate = this.emitPredicate;
            return jumpStep;
        }
    }
}