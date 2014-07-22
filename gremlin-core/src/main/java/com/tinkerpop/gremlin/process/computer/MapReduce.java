package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MapReduce<K, V, OK, OV, R> {

    public String getGlobalVariable();

    public boolean doReduce();

    // TODO: public boolean doMap();
    // TODO: public boolean doCombine();
    // TODO: public default void combine(final K key, final Iterator<V> values, final ReduceEmitter<OK, OV> emitter) { }

    public void map(final Vertex vertex, final MapEmitter<K, V> emitter);

    public default void reduce(final K key, final Iterator<V> values, final ReduceEmitter<OK, OV> emitter) {
    }

    public R getResult(final Iterator<Pair<OK, OV>> keyValues);

    public interface MapEmitter<K, V> {
        public void emit(final K key, final V value);
    }

    public interface ReduceEmitter<OK, OV> {
        public void emit(final OK key, OV value);
    }
}
