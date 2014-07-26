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

    public final String jumpAs;
    public Step jumpToStep;
    public final SPredicate<Traverser<S>> ifPredicate;
    public final SPredicate<Traverser<S>> emitPredicate;
    public final int loops;
    private final boolean jumpBack;

    public JumpStep(final Traversal traversal, final String jumpAs, final SPredicate<Traverser<S>> ifPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.loops = -1;
        this.jumpAs = jumpAs;
        this.ifPredicate = ifPredicate;
        this.emitPredicate = emitPredicate;
        this.jumpToStep = TraversalHelper.asExists(this.jumpAs, this.traversal) ? TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep() : null;
        this.jumpBack = null != this.jumpToStep;
    }

    public JumpStep(final Traversal traversal, final String jumpAs, final SPredicate<Traverser<S>> ifPredicate) {
        this(traversal, jumpAs, ifPredicate, null);
    }

    public JumpStep(final Traversal traversal, final String jumpAs, final int loops, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpAs = jumpAs;
        this.loops = loops;
        this.ifPredicate = t -> t.getLoops() < this.loops;
        this.emitPredicate = emitPredicate;
        this.jumpToStep = TraversalHelper.asExists(this.jumpAs, this.traversal) ? TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep() : null;
        this.jumpBack = null != this.jumpToStep;
    }

    public JumpStep(final Traversal traversal, final String jumpAs, final int loops) {
        this(traversal, jumpAs, loops, null);
    }

    public JumpStep(final Traversal traversal, final String jumpAs) {
        this(traversal, jumpAs, t -> true);
    }

    protected Traverser<S> processNextStart() {
        if (null == this.jumpToStep)
            this.jumpToStep = TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep();
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            if (this.jumpBack) traverser.incrLoops();
            if ((this.loops != -1 && traverser.getLoops() < this.loops) || this.ifPredicate.test(traverser)) {
                traverser.setFuture(this.jumpAs);
                this.jumpToStep.addStarts(new SingleIterator(traverser));
                if (this.emitPredicate != null && this.emitPredicate.test(traverser)) {
                    final Traverser<S> emitTraverser = traverser.makeSibling();
                    if (this.jumpBack) emitTraverser.resetLoops();
                    return emitTraverser;
                }
            } else {
                if (this.jumpBack) traverser.resetLoops();
                return traverser;
            }
        }
    }

    public boolean unRollable() {
        return this.loops != -1 && null == this.emitPredicate;
    }

    public String toString() {
        return this.loops != -1 ? TraversalHelper.makeStepString(this, this.jumpAs, this.loops) : TraversalHelper.makeStepString(this, this.jumpAs);
    }
}