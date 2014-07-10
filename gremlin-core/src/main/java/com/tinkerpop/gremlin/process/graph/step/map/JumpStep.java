package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStep<S> extends MapStep<S, S> {

    public final String jumpAs;
    public final SPredicate<Traverser<S>> ifPredicate;
    public final SPredicate<Traverser<S>> emitPredicate;


    public JumpStep(final Traversal traversal, final String jumpAs, final SPredicate<Traverser<S>> ifPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpAs = jumpAs;
        this.ifPredicate = ifPredicate;
        this.emitPredicate = emitPredicate;
        final Step<?, ?> jumpStep = TraversalHelper.asExists(jumpAs, this.traversal) ? TraversalHelper.getAs(jumpAs, this.traversal) : null;
        this.setFunction(traverser -> {
            if (null != jumpStep)
                traverser.incrLoops();
            if (ifPredicate.test(traverser)) {
                traverser.setFuture(jumpAs);
                if (null == jumpStep)
                    TraversalHelper.getAs(jumpAs, this.traversal).addStarts((Iterator) new SingleIterator<>(traverser));
                else
                    jumpStep.addStarts((Iterator) new SingleIterator<>(traverser));
                return (S) (emitPredicate.test(traverser) ? traverser.get() : NO_OBJECT);
            } else {
                return (S) traverser.get();
            }
        });
    }

    // TODO: Be sure to Traverser.resetLoops() when the object leaves the step. May have to implement processNextStart() and MapStep doesn't allow this.
}