package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphReduce extends Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable> {

    private MapReduce mapReduce;

    public GiraphReduce() {

    }

    @Override
    public void setup(final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
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
    public void reduce(final KryoWritable key, final Iterable<KryoWritable> values, final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<KryoWritable> itty = values.iterator();
        this.mapReduce.reduce(key.get(), new Iterator() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Object next() {
                return itty.next().get();
            }
        }, new GiraphReduceEmitter<>(context));
    }


    public static class GiraphReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

        final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context;
        final KryoWritable<OK> keyWritable = new KryoWritable<>();
        final KryoWritable<OV> valueWritable = new KryoWritable<>();

        public GiraphReduceEmitter(final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final OK key, final OV value) {
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
