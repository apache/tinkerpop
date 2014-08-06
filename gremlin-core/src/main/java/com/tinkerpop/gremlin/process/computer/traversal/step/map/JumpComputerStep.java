package com.tinkerpop.gremlin.process.computer.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpComputerStep<S> extends AbstractStep<S, S> {

    public String jumpAs;
    public Queue<Traverser<S>> queue = new LinkedList<>();
    public SPredicate<Traverser<S>> ifPredicate;
    public int loops = -1;
    public SPredicate<Traverser<S>> emitPredicate;
    private AtomicBoolean jumpBack = null;

    public JumpComputerStep(final Traversal traversal, final JumpStep jumpStep) {
        super(traversal);
        this.jumpAs = jumpStep.jumpAs;
        this.ifPredicate = jumpStep.ifPredicate;
        this.loops = jumpStep.loops;
        this.emitPredicate = jumpStep.emitPredicate;
        if (TraversalHelper.isLabeled(jumpStep))
            this.setAs(jumpStep.getAs());
    }

    protected Traverser<S> processNextStart() {
        final String loopFuture = TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep().getAs();
        if (null == this.jumpBack)
            this.jumpBack = new AtomicBoolean(this.traversal.getSteps().indexOf(this) > this.traversal.getSteps().indexOf(TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep()));
        while (true) {
            if (!this.queue.isEmpty()) {
                return this.queue.remove();
            } else {
                final Traverser<S> traverser = this.starts.next();
                if (this.jumpBack.get()) traverser.incrLoops();
                if (doJump(traverser)) {
                    traverser.setFuture(loopFuture);
                    this.queue.add(traverser);
                    if (null != this.emitPredicate && this.emitPredicate.test(traverser)) {
                        final Traverser<S> emitTraverser = traverser.makeSibling();
                        if (this.jumpBack.get()) emitTraverser.resetLoops();
                        emitTraverser.setFuture(this.nextStep.getAs());
                        this.queue.add(emitTraverser);
                    }
                } else {
                    traverser.setFuture(this.nextStep.getAs());
                    if (this.jumpBack.get()) traverser.resetLoops();
                    this.queue.add(traverser);
                }
            }
        }
    }

    private boolean doJump(final Traverser traverser) {
        return null == this.ifPredicate ? traverser.getLoops() < this.loops : this.ifPredicate.test(traverser);
    }

    public Traverser<S> next() {
        if (this.available) {
            this.available = false;
            return this.nextEnd;
        } else {
            return this.processNextStart();
        }
    }

    public boolean hasNext() {
        if (this.available)
            return true;
        else {
            try {
                this.nextEnd = this.processNextStart();
                this.available = true;
                return true;
            } catch (final NoSuchElementException e) {
                this.available = false;
                return false;
            }
        }
    }
}