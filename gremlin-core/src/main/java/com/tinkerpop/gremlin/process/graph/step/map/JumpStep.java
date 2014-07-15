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
    public Step jumpStep;
    public final SPredicate<Traverser<S>> ifPredicate;
    public final SPredicate<Traverser<S>> emitPredicate;

    public JumpStep(final Traversal traversal, final String jumpAs, final SPredicate<Traverser<S>> ifPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpAs = jumpAs;
        this.ifPredicate = ifPredicate;
        this.emitPredicate = emitPredicate;
        this.jumpStep = TraversalHelper.asExists(this.jumpAs, this.traversal) ? TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep() : null;
    }

    protected Traverser<S> processNextStart() {
        if (null == this.jumpStep)
            this.jumpStep = TraversalHelper.getAs(this.jumpAs, this.traversal).getNextStep();
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            traverser.incrLoops();
            if (this.ifPredicate.test(traverser)) {
                final Traverser<S> ifTraverser = traverser.makeSibling();
                ifTraverser.setFuture(this.jumpAs);
                jumpStep.addStarts(new SingleIterator(ifTraverser));
                if (this.emitPredicate.test(traverser)) {
                    final Traverser<S> emitTraverser = traverser.makeSibling();
                    emitTraverser.resetLoops();
                    return emitTraverser;
                }
            } else {
                final Traverser<S> emitTraverser = traverser.makeSibling();
                emitTraverser.resetLoops();
                return emitTraverser;
            }
        }
    }
}