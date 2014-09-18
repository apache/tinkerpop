package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStep<S> extends AbstractStep<S, S> implements EngineDependent {

    public final String jumpLabel;
    public Step jumpToStep;
    public final SPredicate<Traverser<S>> ifPredicate;
    public final SPredicate<Traverser<S>> emitPredicate;
    public final int loops;
    private AtomicBoolean jumpBack;
    private boolean onGraphComputer = false;
    public Queue<Traverser<S>> queue;

    public JumpStep(final Traversal traversal, final String jumpLabel, final SPredicate<Traverser<S>> ifPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.loops = -1;
        this.jumpLabel = jumpLabel;
        this.ifPredicate = ifPredicate;
        this.emitPredicate = emitPredicate;
        this.futureSetByChild = true;
    }

    public JumpStep(final Traversal traversal, final String jumpLabel, final SPredicate<Traverser<S>> ifPredicate) {
        this(traversal, jumpLabel, ifPredicate, null);
    }

    public JumpStep(final Traversal traversal, final String jumpLabel, final int loops, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpLabel = jumpLabel;
        this.loops = loops;
        this.ifPredicate = null;
        this.emitPredicate = emitPredicate;
        this.futureSetByChild = true;
    }

    public JumpStep(final Traversal traversal, final String jumpLabel, final int loops) {
        this(traversal, jumpLabel, loops, null);
    }

    public JumpStep(final Traversal traversal, final String jumpLabel) {
        this(traversal, jumpLabel, t -> true);
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
            this.jumpBack = new AtomicBoolean(TraversalHelper.relativeLabelDirection(this, this.jumpLabel) == -1);
            // TODO: getNextStep() may be dependent on whether its a jump back or a jump forward
        }
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            if (this.jumpBack.get()) ((Traverser.System<S>) traverser).incrLoops();
            if (doJump(traverser)) {
                ((Traverser.System<S>) traverser).setFuture(this.jumpLabel);
                this.jumpToStep.addStarts(new SingleIterator(traverser));
                if (this.emitPredicate != null && this.emitPredicate.test(traverser)) {
                    final Traverser<S> emitTraverser = ((Traverser.System<S>) traverser).makeSibling();
                    if (this.jumpBack.get()) ((Traverser.System<S>) emitTraverser).resetLoops();
                    ((Traverser.System<S>) emitTraverser).setFuture(this.getNextStep().getLabel());
                    return emitTraverser;
                }
            } else {
                if (this.jumpBack.get()) ((Traverser.System<S>) traverser).resetLoops();
                ((Traverser.System<S>) traverser).setFuture(this.getNextStep().getLabel());
                return traverser;
            }
        }
    }

    private Traverser<S> computerAlgorithm() {
        final String loopFuture = TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep().getLabel();
        if (null == this.jumpBack)
            this.jumpBack = new AtomicBoolean(this.traversal.getSteps().indexOf(this) > this.traversal.getSteps().indexOf(TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep()));
        while (true) {
            if (!this.queue.isEmpty()) {
                return this.queue.remove();
            } else {
                final Traverser.System<S> traverser = this.starts.next();
                if (this.jumpBack.get()) traverser.incrLoops();
                if (doJump(traverser)) {
                    traverser.setFuture(loopFuture);
                    this.queue.add(traverser);
                    if (null != this.emitPredicate && this.emitPredicate.test(traverser)) {
                        final Traverser.System<S> emitTraverser = traverser.makeSibling();
                        if (this.jumpBack.get()) emitTraverser.resetLoops();
                        emitTraverser.setFuture(this.nextStep.getLabel());
                        this.queue.add(emitTraverser);
                    }
                } else {
                    traverser.setFuture(this.nextStep.getLabel());
                    if (this.jumpBack.get()) traverser.resetLoops();
                    this.queue.add(traverser);
                }
            }
        }
    }

    public boolean unRollable() {
        return !this.onGraphComputer && this.loops != -1 && null == this.emitPredicate;
    }

    private boolean doJump(final Traverser traverser) {
        return null == this.ifPredicate ? traverser.getLoops() < this.loops : this.ifPredicate.test(traverser);
    }

    public String toString() {
        return this.loops != -1 ?
                TraversalHelper.makeStepString(this, this.jumpLabel, this.loops) :
                TraversalHelper.makeStepString(this, this.jumpLabel);
    }
}