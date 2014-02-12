package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStep<S> extends MapStep<S, S> {

    public JumpStep(final Traversal traversal, final String as, final Predicate<Holder<S>> ifPredicate, final Predicate<Holder<S>> emitPredicate) {
        super(traversal);
        final Step<?, ?> jumpStep = GremlinHelper.asExists(as, this.traversal) ? GremlinHelper.getAs(as, this.traversal) : null;
        this.setFunction(holder -> {
            if (null != jumpStep)
                holder.incrLoops();
            if (ifPredicate.test(holder)) {
                holder.setFuture(as);
                if (null == jumpStep)
                    GremlinHelper.getAs(as, this.traversal).addStarts((Iterator) new SingleIterator<>(holder));
                else
                    jumpStep.addStarts((Iterator) new SingleIterator<>(holder));
                return (S) (emitPredicate.test(holder) ? holder.get() : NO_OBJECT);
            } else {
                return (S) holder.get();
            }
        });
    }
}