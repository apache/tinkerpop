package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MapReduce<K, V, OK, OV, R> {

    public static class NullObject implements Comparable<NullObject>, Serializable {
        private static final NullObject INSTANCE = new NullObject();

        public static NullObject get() {
            return INSTANCE;
        }

        public int hashCode() {
            return 666666666;
        }

        public boolean equals(final Object object) {
            return object instanceof NullObject;
        }

        public int compareTo(final NullObject nullObject) {
            return 0;
        }
    }

    public static enum Stage {MAP, COMBINE, REDUCE}


    public default void stageConfiguration(final Configuration configuration) {

    }

    public default void setup(final Configuration configuration) {

    }

    public String getResultVariable();

    public boolean doStage(final Stage stage);

    public default void map(final Vertex vertex, final MapEmitter<K, V> emitter) {
    }

    public default void combine(final K key, final Iterator<V> values, final ReduceEmitter<OK, OV> emitter) {
    }

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
