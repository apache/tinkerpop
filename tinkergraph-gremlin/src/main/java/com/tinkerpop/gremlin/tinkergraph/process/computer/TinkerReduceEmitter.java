package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.javatuples.Pair;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

    public Queue<Pair<OK, OV>> resultList = new ConcurrentLinkedQueue<>();

    @Override
    public void emit(final OK key, final OV value) {
        this.resultList.add(new Pair<>(key, value));
    }
}
