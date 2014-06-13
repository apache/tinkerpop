package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStep<S> extends MapStep<S, S> {

    public JumpStep(final Traversal traversal, final String as, final SPredicate<Traverser<S>> ifPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        final Step<?, ?> jumpStep = TraversalHelper.asExists(as, this.traversal) ? TraversalHelper.getAs(as, this.traversal) : null;
        this.setFunction(traverser -> {
            if (null != jumpStep)
                traverser.incrLoops();
            if (ifPredicate.test(traverser)) {
                traverser.setFuture(as);
                if (null == jumpStep)
                    TraversalHelper.getAs(as, this.traversal).addStarts((Iterator) new SingleIterator<>(traverser));
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