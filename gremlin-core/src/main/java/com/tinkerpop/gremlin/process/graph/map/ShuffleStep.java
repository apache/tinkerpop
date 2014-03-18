package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.UnHolderIterator;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ShuffleStep<S> extends FlatMapStep<S, S> {

    public ShuffleStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(holder -> {
            final List<Holder<S>> list = new ArrayList<>();
            list.add(holder);
            list.addAll(StreamFactory.stream(getPreviousStep()).collect(Collectors.<Holder<S>>toList()));
            Collections.shuffle(list);
            return new UnHolderIterator<>(list.iterator());
        });
    }
}
