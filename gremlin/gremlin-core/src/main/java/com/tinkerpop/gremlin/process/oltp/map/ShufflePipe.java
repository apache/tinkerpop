package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.util.UnHolderIterator;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ShufflePipe<S> extends FlatMapPipe<S, S> {

    public ShufflePipe(final Traversal pipeline) {
        super(pipeline);
        this.setFunction(holder -> {
            final List<Holder<S>> list = new ArrayList<>();
            list.add(holder);
            list.addAll(StreamFactory.stream(getPreviousPipe()).collect(Collectors.<Holder<S>>toList()));
            Collections.shuffle(list);
            return new UnHolderIterator<>(list.iterator());
        });
    }
}
