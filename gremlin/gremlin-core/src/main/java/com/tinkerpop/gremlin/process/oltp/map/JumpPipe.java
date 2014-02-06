package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpPipe<S> extends MapPipe<S, S> {

    public JumpPipe(final Traversal pipeline, final String as, final Predicate<Holder<S>> ifPredicate, final Predicate<Holder<S>> emitPredicate) {
        super(pipeline);
        final Pipe<?, ?> jumpPipe = GremlinHelper.asExists(as, this.pipeline) ? GremlinHelper.getAs(as, this.pipeline) : null;
        this.setFunction(holder -> {
            if (null != jumpPipe)
                holder.incrLoops();
            if (ifPredicate.test(holder)) {
                holder.setFuture(as);
                if (null == jumpPipe)
                    GremlinHelper.getAs(as, this.pipeline).addStarts((Iterator) new SingleIterator<>(holder));
                else
                    jumpPipe.addStarts((Iterator) new SingleIterator<>(holder));
                return (S) (emitPredicate.test(holder) ? holder.get() : NO_OBJECT);
            } else {
                return (S) holder.get();
            }
        });
    }
}