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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpComputerStep<S> extends AbstractStep<S, S> {

    public String jumpAs;
    public Queue<Traverser<S>> queue = new LinkedList<>();
    public SPredicate<Traverser<S>> ifPredicate;
    public SPredicate<Traverser<S>> emitPredicate;

    public JumpComputerStep(final Traversal traversal, final JumpStep<S> jumpStep) {
        super(traversal);
        this.jumpAs = jumpStep.jumpAs;
        this.ifPredicate = jumpStep.ifPredicate;
        this.emitPredicate = jumpStep.emitPredicate;
    }

    protected Traverser<S> processNextStart() {
        while (true) {
            if (!this.queue.isEmpty()) {
                return this.queue.remove();
            } else {
                final Traverser<S> traverser = this.starts.next();
                traverser.incrLoops();
                if (this.ifPredicate.test(traverser)) {
                    final Traverser<S> loopTraverser = traverser.makeChild(this.getAs(), traverser.get());
                    loopTraverser.setFuture(TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep().getAs());
                    this.queue.add(loopTraverser);
                    if (this.emitPredicate.test(traverser)) {
                        final Traverser<S> emitTraverser = traverser.makeChild(this.getAs(), traverser.get());
                        emitTraverser.setFuture(this.nextStep.getAs());
                        this.queue.add(emitTraverser);
                    }
                } else {
                    final Traverser<S> emitTraverser = traverser.makeChild(this.getAs(), traverser.get());
                    emitTraverser.setFuture(this.nextStep.getAs());
                    this.queue.add(emitTraverser);
                }
            }
        }
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