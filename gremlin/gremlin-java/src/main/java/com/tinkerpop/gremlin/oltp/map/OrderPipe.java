package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.NonHolderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderPipe<S> extends FlatMapPipe<S, S> {

    public Comparator<Holder<S>> comparator;

    public OrderPipe(final Pipeline pipeline, final Comparator<Holder<S>> comparator) {
        super(pipeline);
        this.comparator = comparator;
        this.setFunction(holder -> {
            final List<Holder<S>> list = new ArrayList<>();
            list.add(holder);
            list.addAll(StreamFactory.stream(getPreviousPipe()).collect(Collectors.<Holder<S>>toList()));
            Collections.sort(list, this.comparator);
            return new NonHolderIterator<>(list.iterator());
        });
    }
}
