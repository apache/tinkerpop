package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    final Mapper.Context context;
    final KryoWritable<K> keyWritable = new KryoWritable<>();
    final KryoWritable<V> valueWritable = new KryoWritable<>();

    public GiraphMapEmitter(final Mapper.Context context) {
        this.context = context;
    }

    public void emit(final K key, final V value) {
        this.keyWritable.set(key);
        this.valueWritable.set(value);
        try {
            this.context.write(this.keyWritable, this.valueWritable);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
