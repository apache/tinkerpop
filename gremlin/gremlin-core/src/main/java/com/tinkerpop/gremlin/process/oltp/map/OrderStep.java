package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.UnHolderIterator;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import com.tinkerpop.gremlin.process.Holder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderStep<S> extends FlatMapStep<S, S> {

    public Comparator<Holder<S>> comparator;

    public OrderStep(final Traversal traversal, final Comparator<Holder<S>> comparator) {
        super(traversal);
        this.comparator = comparator;
        this.setFunction(holder -> {
            final List<Holder<S>> list = new ArrayList<>();
            list.add(holder);
            list.addAll(StreamFactory.stream(getPreviousStep()).collect(Collectors.<Holder<S>>toList()));
            Collections.sort(list, this.comparator);
            return new UnHolderIterator<>(list.iterator());
        });
    }
}
