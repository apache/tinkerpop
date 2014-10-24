package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

    protected Queue<Pair<OK, OV>> reduceQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void emit(final OK key, final OV value) {
        this.reduceQueue.add(new Pair<>(key, value));
    }

    protected void complete(final MapReduce<?, ?, OK, OV, ?> mapReduce) {
        if (mapReduce.getReduceKeySort().isPresent()) {
            final Comparator<OK> comparator = mapReduce.getReduceKeySort().get();
            final List<Pair<OK, OV>> list = new ArrayList<>(this.reduceQueue);
            Collections.sort(list, Comparator.comparing(Pair::getValue0, comparator));
            this.reduceQueue.clear();
            this.reduceQueue.addAll(list);
        }
    }
}
