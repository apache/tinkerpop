package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.AbstractPipe;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ShufflePipe<E> extends AbstractPipe<E, E> {

    private Iterator<Holder<E>> iterator = null;

    public ShufflePipe(final Pipeline pipeline) {
        super(pipeline);
    }

    public Holder<E> processNextStart() {
        if (null == this.iterator) {
            final List<Holder<E>> list = StreamFactory.stream(this.starts).collect(Collectors.<Holder<E>>toList());
            Collections.shuffle(list);
        }
        return this.iterator.next();
    }
}
