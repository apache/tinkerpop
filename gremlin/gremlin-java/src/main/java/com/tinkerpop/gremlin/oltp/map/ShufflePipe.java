package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.UnHolderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ShufflePipe<S> extends FlatMapPipe<S, S> {

    public ShufflePipe(final Pipeline pipeline) {
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
