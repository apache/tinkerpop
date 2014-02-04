package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.SingleIterator;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpPipe<S> extends MapPipe<S, S> {

    public JumpPipe(final Pipeline pipeline, final String as, final Predicate<Holder<S>> ifPredicate, final Predicate<Holder<S>> emitPredicate) {
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
                return (S) (emitPredicate.test(holder) ? holder.get() : Pipe.NO_OBJECT);
            } else {
                return (S) holder.get();
            }
        });
    }
}