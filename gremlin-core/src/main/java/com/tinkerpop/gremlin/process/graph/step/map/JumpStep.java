package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStep<S> extends AbstractStep<S, S> {

    public final String jumpLabel;
    public Step jumpToStep;
    public final SPredicate<Traverser<S>> ifPredicate;
    public final SPredicate<Traverser<S>> emitPredicate;
    public final int loops;
    private final boolean jumpBack;

    public JumpStep(final Traversal traversal, final String jumpLabel, final SPredicate<Traverser<S>> ifPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.loops = -1;
        this.jumpLabel = jumpLabel;
        this.ifPredicate = ifPredicate;
        this.emitPredicate = emitPredicate;
        this.jumpToStep = TraversalHelper.hasLabel(this.jumpLabel, this.traversal) ? TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep() : null;
        this.jumpBack = null != this.jumpToStep;
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
        this.jumpToStep = TraversalHelper.hasLabel(this.jumpLabel, this.traversal) ? TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep() : null;
        this.jumpBack = null != this.jumpToStep;
        this.futureSetByChild = true;
    }

    public JumpStep(final Traversal traversal, final String jumpLabel, final int loops) {
        this(traversal, jumpLabel, loops, null);
    }

    public JumpStep(final Traversal traversal, final String jumpLabel) {
        this(traversal, jumpLabel, t -> true);
    }

    protected Traverser<S> processNextStart() {
        if (null == this.jumpToStep)
            this.jumpToStep = TraversalHelper.getStep(this.jumpLabel, this.traversal).getNextStep();
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            if (this.jumpBack) traverser.incrLoops();
            if (doJump(traverser)) {
                traverser.setFuture(this.jumpLabel);
                this.jumpToStep.addStarts(new SingleIterator(traverser));
                if (this.emitPredicate != null && this.emitPredicate.test(traverser)) {
                    final Traverser<S> emitTraverser = traverser.makeSibling();
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

    public boolean unRollable() {
        return this.loops != -1 && null == this.emitPredicate;
    }

    private boolean doJump(final Traverser traverser) {
        return null == this.ifPredicate ? traverser.getLoops() < this.loops : this.ifPredicate.test(traverser);
    }

    public String toString() {
        return this.loops != -1 ? TraversalHelper.makeStepString(this, this.jumpLabel, this.loops) : TraversalHelper.makeStepString(this, this.jumpLabel);
    }
}