package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import org.javatuples.Pair;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStep<S> extends AbstractStep<S, S> implements EngineDependent {

    public String jumpLabel;
    public Step jumpToStep;

    public Predicate<Traverser<S>> jumpPredicate;
    public Pair<Short, Compare> jumpLoops;
    public Boolean jumpChoice;

    public Predicate<Traverser<S>> emitPredicate;
    public Boolean emitChoice;

    private Boolean jumpBack;
    private boolean onGraphComputer = false;
    public Queue<Traverser<S>> queue;
    public boolean doWhile = true;

    public JumpStep(final Traversal traversal) {
        super(traversal);
        this.futureSetByChild = true;
    }

    public void onEngine(final Engine engine) {
        if (engine.equals(Engine.COMPUTER)) {
            this.onGraphComputer = true;
            this.queue = new LinkedList<>();
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
            final Traverser.System<S> traverser = this.starts.next();
            if (this.jumpBack) traverser.incrLoops();
            if (doJump(traverser)) {
                traverser.setFuture(this.jumpLabel);
                this.jumpToStep.addStarts(new SingleIterator(traverser));
                if (this.emitPredicate != null && this.emitPredicate.test(traverser)) {
                    final Traverser.System<S> emitTraverser = traverser.makeSibling();
                    if (this.jumpBack) emitTraverser.resetLoops();
                    emitTraverser.setFuture(this.getNextStep().getLabel());
                    return emitTraverser;
                }
            } else {
                if (this.jumpBack) traverser.resetLoops();
                traverser.setFuture(this.getNextStep().getLabel());
                return traverser;
            }
        }
    }

    private Traverser<S> computerAlgorithm() {
        final String loopFuture = TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep().getLabel();
        if (null == this.jumpBack)
            this.jumpBack = this.traversal.getSteps().indexOf(this) > this.traversal.getSteps().indexOf(TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep());
        while (true) {
            if (!this.queue.isEmpty()) {
                return this.queue.remove();
            } else {
                final Traverser.System<S> traverser = this.starts.next();
                if (this.jumpBack) traverser.incrLoops();
                if (doJump(traverser)) {
                    traverser.setFuture(loopFuture);
                    this.queue.add(traverser);
                    if (null != this.emitPredicate && this.emitPredicate.test(traverser)) {
                        final Traverser.System<S> emitTraverser = traverser.makeSibling();
                        if (this.jumpBack) emitTraverser.resetLoops();
                        emitTraverser.setFuture(this.nextStep.getLabel());
                        this.queue.add(emitTraverser);
                    }
                } else {
                    traverser.setFuture(this.nextStep.getLabel());
                    if (this.jumpBack) traverser.resetLoops();
                    this.queue.add(traverser);
                }
            }
        }
    }

    public boolean unRollable() {
        return !this.onGraphComputer && this.jumpLoops != null && null == this.emitPredicate;
    }

    public boolean isDoWhile() {
        return this.doWhile;
    }

    private boolean doJump(final Traverser traverser) {
        return null == this.jumpPredicate ? this.jumpLoops.getValue1().test(traverser.getLoops(), this.jumpLoops.getValue0()) : this.jumpPredicate.test(traverser);
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
        private Pair<Short, Compare> jumpLoops = null;
        private Boolean emitChoice = null;
        private Boolean jumpChoice = null;
        private Predicate<Traverser<S>> jumpPredicate = null;
        private Predicate<Traverser<S>> emitPredicate = null;
        private Boolean jumpBack = null;
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
            this.jumpChoice = Boolean.valueOf(jumpChoice);
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
            this.jumpBack = TraversalHelper.hasLabel(jumpLabel, traversal);
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
            jumpStep.jumpBack = this.jumpBack;
            return jumpStep;
        }
    }
}