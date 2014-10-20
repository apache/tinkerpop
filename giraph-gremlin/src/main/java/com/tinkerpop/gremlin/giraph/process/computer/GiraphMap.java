package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMap extends Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable> {

    private MapReduce mapReduce;

    public GiraphMap() {

    }

    @Override
    public void setup(final Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable>.Context context) {
        try {
            final Class<? extends MapReduce> mapReduceClass = context.getConfiguration().getClass(Constants.MAP_REDUCE_CLASS, MapReduce.class, MapReduce.class);
            final Constructor<? extends MapReduce> constructor = mapReduceClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            this.mapReduce = constructor.newInstance();
            this.mapReduce.loadState(ConfUtil.makeApacheConfiguration(context.getConfiguration()));
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable>.Context context) throws IOException, InterruptedException {
        this.mapReduce.map(value.getBaseVertex(), new GiraphMapEmitter<>(context));
    }

    public static class GiraphMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

        final Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable>.Context context;
        final KryoWritable<K> keyWritable = new KryoWritable<>();
        final KryoWritable<V> valueWritable = new KryoWritable<>();

        public GiraphMapEmitter(final Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final K key, final V value) {
            this.keyWritable.set(key);
            this.valueWritable.set(value);
            try {
                this.context.write(this.keyWritable, this.valueWritable);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
